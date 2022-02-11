/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	srov1beta1 "github.com/openshift-psap/special-resource-operator/api/v1beta1"
	"github.com/openshift-psap/special-resource-operator/pkg/assets"
	"github.com/openshift-psap/special-resource-operator/pkg/clients"
	"github.com/openshift-psap/special-resource-operator/pkg/color"
	"github.com/openshift-psap/special-resource-operator/pkg/filter"
	"github.com/openshift-psap/special-resource-operator/pkg/helmer"
	"github.com/openshift-psap/special-resource-operator/pkg/registry"
	"github.com/openshift-psap/special-resource-operator/pkg/resource"
	"github.com/openshift-psap/special-resource-operator/pkg/watcher"
	buildv1 "github.com/openshift/api/build/v1"
	"github.com/pkg/errors"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chartutil"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	semver = `^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`

	SRMgvk        = "SpecialResourceModule"
	SRMOwnedLabel = "specialresourcemodule.openshift.io/owned"
)

var (
	versionRegex = regexp.MustCompile(semver)
)

type Metadata struct {
	OperatingSystem       string                           `json:"operatingSystem"`
	KernelFullVersion     string                           `json:"kernelFullVersion"`
	RTKernelFullVersion   string                           `json:"rtKernelFullVersion"`
	DriverToolkitImage    string                           `json:"driverToolkitImage"`
	ClusterVersion        string                           `json:"clusterVersion"`
	OSImageURL            string                           `json:"osImageURL"`
	GroupName             ResourceGroupName                `json:"groupName"`
	SpecialResourceModule srov1beta1.SpecialResourceModule `json:"specialResourceModule"`
}

type OCPVersionInfo struct {
	KernelVersion   string
	RTKernelVersion string
	DTKImage        string
	OSVersion       string
	OSImage         string
	ClusterVersion  string
}

// SpecialResourceModuleReconciler reconciles a SpecialResourceModule object
type SpecialResourceModuleReconciler struct {
	Log     logr.Logger
	Scheme  *runtime.Scheme
	reg     registry.Registry
	filter  filter.Filter
	watcher watcher.Watcher
}

func getAllResources(kind, apiVersion, namespace, name string) ([]unstructured.Unstructured, error) {
	if name == "" {
		var l unstructured.UnstructuredList
		l.SetKind(kind)
		l.SetAPIVersion(apiVersion)
		err := clients.Interface.List(context.Background(), &l)
		if err != nil {
			return nil, err
		}
		return l.Items, nil
	}
	obj := unstructured.Unstructured{}
	obj.SetKind(kind)
	obj.SetAPIVersion(apiVersion)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	key := client.ObjectKeyFromObject(&obj)
	err := clients.Interface.Get(context.Background(), key, &obj)
	return []unstructured.Unstructured{obj}, err
}

func filterResources(selectors []srov1beta1.SpecialResourceModuleSelector, objs []unstructured.Unstructured) ([]unstructured.Unstructured, error) {
	if len(selectors) == 0 {
		return objs, nil
	}
	filteredObjects := make([]unstructured.Unstructured, 0)
	for _, selector := range selectors {
		for _, obj := range objs {
			candidates, err := watcher.GetJSONPath(selector.Path, obj)
			if err != nil {
				return filteredObjects, err
			}
			found := false
			for _, candidate := range candidates {
				if candidate == selector.Value {
					found = true
					break
				}
			}
			if selector.Exclude {
				found = !found
			}
			if found {
				filteredObjects = append(filteredObjects, obj)
			}
		}
	}
	return filteredObjects, nil
}

func getVersionInfoFromImage(entry string, reg registry.Registry) (OCPVersionInfo, error) {
	manifestsLastLayer, err := reg.LastLayer(entry)
	if err != nil {
		return OCPVersionInfo{}, err
	}
	version, dtkURL, err := reg.ReleaseManifests(manifestsLastLayer)
	if err != nil {
		return OCPVersionInfo{}, err
	}
	dtkLastLayer, err := reg.LastLayer(dtkURL)
	if err != nil {
		return OCPVersionInfo{}, err
	}
	dtkEntry, err := reg.ExtractToolkitRelease(dtkLastLayer)
	if err != nil {
		return OCPVersionInfo{}, err
	}
	return OCPVersionInfo{
		KernelVersion:   dtkEntry.KernelFullVersion,
		RTKernelVersion: dtkEntry.RTKernelFullVersion,
		DTKImage:        dtkURL,
		OSVersion:       dtkEntry.OSVersion,
		OSImage:         entry,
		ClusterVersion:  version,
	}, nil
}

func getImageFromVersion(entry string) (string, error) {
	type versionNode struct {
		Version string `json:"version"`
		Payload string `json:"payload"`
	}
	type versionGraph struct {
		Nodes []versionNode `json:"nodes"`
	}
	res := versionRegex.FindStringSubmatch(entry)
	full, major, minor := res[0], res[1], res[2]
	var imageURL string
	{
		transport, _ := transport.HTTPWrappersForConfig(
			&transport.Config{
				UserAgent: rest.DefaultKubernetesUserAgent() + "(release-info)",
			},
			http.DefaultTransport,
		)
		client := &http.Client{Transport: transport}
		u, _ := url.Parse("https://api.openshift.com/api/upgrades_info/v1/graph")
		for _, stream := range []string{"fast", "stable", "candidate"} {
			u.RawQuery = url.Values{"channel": []string{fmt.Sprintf("%s-%s.%s", stream, major, minor)}}.Encode()
			if err := func() error {
				req, err := http.NewRequest("GET", u.String(), nil)
				if err != nil {
					return err
				}
				req.Header.Set("Accept", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
				default:
					io.Copy(ioutil.Discard, resp.Body)
					return fmt.Errorf("unable to retrieve image. status code %d", resp.StatusCode)
				}
				data, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				var versions versionGraph
				if err := json.Unmarshal(data, &versions); err != nil {
					return err
				}
				for _, version := range versions.Nodes {
					if version.Version == full && len(version.Payload) > 0 {
						imageURL = version.Payload
						break
					}
				}

				return nil
			}(); err != nil {
				return "", err
			}
		}
		if len(imageURL) == 0 {
			return imageURL, fmt.Errorf("version %s not found", entry)
		}
	}
	return imageURL, nil
}

func (r *SpecialResourceModuleReconciler) getOCPVersions(watchList []srov1beta1.SpecialResourceModuleWatch) (map[string]OCPVersionInfo, error) {
	logVersion := r.Log.WithName(color.Print("versions", color.Purple))
	versionMap := make(map[string]OCPVersionInfo)
	for _, resource := range watchList {
		// get all resources -> filter them -> keep working.
		// so lets get to it.

		objs, err := getAllResources(resource.Kind, resource.ApiVersion, resource.Namespace, resource.Name)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		logVersion.Info("pre filter", "len", len(objs))
		objs, err = filterResources(resource.Selector, objs)
		if err != nil {
			logVersion.Error(err, "something is quite off")
			return nil, err
		}
		logVersion.Info("post filter", "len", len(objs))
		for _, obj := range objs {
			result, err := watcher.GetJSONPath(resource.Path, obj)
			if err != nil {
				logVersion.Error(err, "Error when looking for path. Continue", "name", obj.GetName(), "path", resource.Path)
				continue
			}
			for _, element := range result {
				var image string
				if versionRegex.MatchString(element) {
					tmp, err := getImageFromVersion(element)
					if err != nil {
						return nil, err
					}
					logVersion.Info("Version from regex", "name", obj.GetName(), "element", element)
					image = tmp
				} else if strings.Contains(element, "@") || strings.Contains(element, ":") {
					logVersion.Info("Version from image", "name", obj.GetName(), "element", element)
					image = element
				} else {
					return nil, fmt.Errorf("format error. %s is not a valid image/version", element)
				}
				info, err := getVersionInfoFromImage(image, r.reg)
				if err != nil {
					return nil, err
				}
				versionMap[info.ClusterVersion] = info
			}
		}
	}
	return versionMap, nil
}

func createNamespace(r srov1beta1.SpecialResourceModule) error {

	ns := []byte(`apiVersion: v1
kind: Namespace
metadata:
  annotations:
    specialresource.openshift.io/wait: "true"
    openshift.io/cluster-monitoring: "true"
  name: `)

	if r.Spec.Namespace != "" {
		add := []byte(r.Spec.Namespace)
		ns = append(ns, add...)
	} else {
		r.Spec.Namespace = r.Name
		add := []byte(r.Spec.Namespace)
		ns = append(ns, add...)
	}
	return resource.CreateFromYAML(ns, false, &r, r.Name, "", nil, "", "")
}

func getMetadata(srm srov1beta1.SpecialResourceModule, info OCPVersionInfo) Metadata {
	return Metadata{
		OperatingSystem:     info.OSVersion,
		KernelFullVersion:   info.KernelVersion,
		RTKernelFullVersion: info.RTKernelVersion,
		DriverToolkitImage:  info.DTKImage,
		ClusterVersion:      info.ClusterVersion,
		OSImageURL:          info.OSImage,
		GroupName: ResourceGroupName{
			DriverBuild:            "driver-build",
			DriverContainer:        "driver-container",
			RuntimeEnablement:      "runtime-enablement",
			DevicePlugin:           "device-plugin",
			DeviceMonitoring:       "device-monitoring",
			DeviceDashboard:        "device-dashboard",
			DeviceFeatureDiscovery: "device-feature-discovery",
			CSIDriver:              "csi-driver",
		},
		SpecialResourceModule: srm,
	}
}

func reconcileChart(srm *srov1beta1.SpecialResourceModule, metadata Metadata, reconciledInput []string) ([]string, error) {
	reconciledInputMap := make(map[string]bool)
	for _, element := range reconciledInput {
		reconciledInputMap[element] = true
	}
	result := make([]string, 0)
	c, err := helmer.Load(srm.Spec.Chart)
	if err != nil {
		return result, err
	}

	nostate := *c
	nostate.Templates = []*chart.File{}
	stateYAMLS := []*chart.File{}
	for _, template := range c.Templates {
		if assets.ValidStateName(template.Name) {
			if _, ok := reconciledInputMap[template.Name]; !ok {
				stateYAMLS = append(stateYAMLS, template)
			} else {
				result = append(result, template.Name)
			}
		} else {
			nostate.Templates = append(nostate.Templates, template)
		}
	}

	sort.Slice(stateYAMLS, func(i, j int) bool {
		return stateYAMLS[i].Name < stateYAMLS[j].Name
	})

	for _, stateYAML := range stateYAMLS {
		step := nostate
		step.Templates = append(nostate.Templates, stateYAML)

		step.Values, err = chartutil.CoalesceValues(&step, srm.Spec.Set.Object)
		if err != nil {
			return result, err
		}

		rinfo, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&metadata)
		if err != nil {
			return result, err
		}
		step.Values, err = chartutil.CoalesceValues(&step, rinfo)
		if err != nil {
			return result, err
		}
		err = helmer.Run(step, step.Values,
			srm,
			srm.Name,
			srm.Spec.Namespace,
			nil,
			metadata.KernelFullVersion,
			metadata.OperatingSystem,
			false)
		if err != nil {
			return result, err
		}
		result = append(result, stateYAML.Name)
	}
	return nil, nil
}

func FindSRM(a []srov1beta1.SpecialResourceModule, x string) (int, bool) {
	for i, n := range a {
		if x == n.GetName() {
			return i, true
		}
	}
	return -1, false
}

func updateSpecialResourceModuleStatus(resource srov1beta1.SpecialResourceModule) error {
	return clients.Interface.StatusUpdate(context.Background(), &resource)
}

func NewSpecialResourceModuleReconciler(log logr.Logger, scheme *runtime.Scheme, reg registry.Registry, f filter.Filter) SpecialResourceModuleReconciler {
	return SpecialResourceModuleReconciler{
		Log:    log,
		Scheme: scheme,
		reg:    reg,
		filter: f,
	}
}

// Reconcile Reconiliation entry point
func (r *SpecialResourceModuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logModule := r.Log.WithName(color.Print("reconcile: "+req.Name, color.Purple))
	logModule.Info("Reconciling")

	srm := &srov1beta1.SpecialResourceModuleList{}

	opts := []client.ListOption{}
	err := clients.Interface.List(context.Background(), srm, opts...)
	if err != nil {
		return reconcile.Result{}, err
	}

	var request int
	var found bool
	if request, found = FindSRM(srm.Items, req.Name); !found {
		logModule.Info("Not found")
		return reconcile.Result{}, nil
	}
	resource := srm.Items[request]

	if resource.GetDeletionTimestamp() != nil {
		logModule.Info("Deleted resource")
		//TODO i need to clean the watches.
		return reconcile.Result{}, nil
	}

	if err := r.watcher.ReconcileWatches(resource); err != nil {
		logModule.Error(err, "failed to update watched resources")
		return reconcile.Result{}, err
	}

	_ = createNamespace(resource)

	//TODO cache images, wont change dynamically.
	clusterVersions, err := r.getOCPVersions(resource.Spec.Watch)
	if err != nil {
		return reconcile.Result{}, err
	}

	if resource.Status.Versions == nil {
		resource.Status.Versions = make(map[string]srov1beta1.SpecialResourceModuleVersionStatus)
	}

	updateList := make([]OCPVersionInfo, 0)
	deleteList := make([]OCPVersionInfo, 0)
	for resourceVersion, _ := range resource.Status.Versions {
		if data, ok := clusterVersions[resourceVersion]; ok {
			updateList = append(updateList, data)
		} else {
			deleteList = append(deleteList, data)
		}
	}
	for _, clusterInfo := range clusterVersions {
		updateList = append(updateList, clusterInfo)
	}

	for _, element := range deleteList {
		logModule.Info("Removing version", "version", element.ClusterVersion)
		//TODO
	}
	for _, element := range updateList {
		logModule.Info("Reconciling version", "version", element.ClusterVersion)
		metadata := getMetadata(resource, element)
		var inputList []string
		if data, ok := resource.Status.Versions[element.ClusterVersion]; ok {
			inputList = data.ReconciledTemplates
		}
		reconciledList, err := reconcileChart(&resource, metadata, inputList)
		resource.Status.Versions[element.ClusterVersion] = srov1beta1.SpecialResourceModuleVersionStatus{
			ReconciledTemplates: reconciledList,
			Complete:            len(reconciledList) == 0,
		}
		if e := updateSpecialResourceModuleStatus(resource); e != nil {
			return reconcile.Result{}, e
		}
		if err != nil {
			return reconcile.Result{}, err
		}

	}

	logModule.Info("Done")
	return reconcile.Result{}, nil
}

// SetupWithManager main initalization for manager
func (r *SpecialResourceModuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	platform, err := clients.Interface.GetPlatform()
	if err != nil {
		return err
	}

	if platform == "OCP" {
		c, err := ctrl.NewControllerManagedBy(mgr).
			For(&srov1beta1.SpecialResourceModule{}).
			Owns(&buildv1.BuildConfig{}).
			WithOptions(controller.Options{
				MaxConcurrentReconciles: 1,
			}).
			WithEventFilter(r.filter.GetPredicates()).
			Build(r)

		r.watcher = watcher.New(c)
		return err
	}
	return errors.New("SpecialResourceModules only work in OCP")
}
