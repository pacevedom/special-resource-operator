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
	"github.com/openshift-psap/special-resource-operator/pkg/watcher"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"

	"github.com/go-logr/logr"
	srov1beta1 "github.com/openshift-psap/special-resource-operator/api/v1beta1"
	"github.com/openshift-psap/special-resource-operator/pkg/clients"
	"github.com/openshift-psap/special-resource-operator/pkg/color"
	"github.com/openshift-psap/special-resource-operator/pkg/filter"
	"github.com/openshift-psap/special-resource-operator/pkg/registry"
	buildv1 "github.com/openshift/api/build/v1"
	"github.com/pkg/errors"

	imagev1 "github.com/openshift/api/image/v1"

	"github.com/oliveagle/jsonpath"
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
	logModule    logr.Logger
	versionRegex = regexp.MustCompile(semver)
)

func createImageStream(name, namespace string) error {
	//TODO
	return nil
}

type OCPVersionInfo struct {
	KernelVersion   string
	RTKernelVersion string
	DTKImage        string
}

func getResource(kind, apiVersion, namespace, name string) (unstructured.Unstructured, error) {
	obj := unstructured.Unstructured{}
	obj.SetKind(kind)
	obj.SetAPIVersion(apiVersion)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	key := client.ObjectKeyFromObject(&obj)
	err := clients.Interface.Get(context.Background(), key, &obj)
	return obj, err
}

func getJSONPath(path string, obj unstructured.Unstructured) ([]string, error) {
	expression, err := jsonpath.Compile(path)
	if err != nil {
		return nil, err
	}
	match, err := expression.Lookup(obj.Object)
	if err != nil {
		return nil, err
	}
	switch reflect.TypeOf(match).Kind() {
	case reflect.Slice:
		if res, ok := match.([]interface{}); !ok {
			return nil, errors.New("Error converting result to string")
		} else {
			strSlice := make([]string, 0)
			for _, element := range res {
				strSlice = append(strSlice, element.(string))
			}
			return strSlice, nil
		}
	case reflect.String:
		return []string{match.(string)}, nil
	}
	return nil, errors.New("Unsupported result")
}

func getVersionInfoFromImage(entry string, reg registry.Registry) (string, OCPVersionInfo, error) {
	manifestsLastLayer, err := reg.LastLayer(entry)
	if err != nil {
		return "", OCPVersionInfo{}, err
	}
	version, dtkURL, err := reg.ReleaseManifests(manifestsLastLayer)
	if err != nil {
		return "", OCPVersionInfo{}, err
	}
	dtkLastLayer, err := reg.LastLayer(dtkURL)
	if err != nil {
		return "", OCPVersionInfo{}, err
	}
	dtkEntry, err := reg.ExtractToolkitRelease(dtkLastLayer)
	if err != nil {
		return "", OCPVersionInfo{}, err
	}
	return version, OCPVersionInfo{
		KernelVersion:   dtkEntry.KernelFullVersion,
		RTKernelVersion: dtkEntry.RTKernelFullVersion,
		DTKImage:        dtkURL,
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
	}
	return imageURL, nil
}

func getOCPVersions(watchList []srov1beta1.SpecialResourceModuleWatch, reg registry.Registry) (map[string]OCPVersionInfo, error) {
	versionMap := make(map[string]OCPVersionInfo)
	for _, resource := range watchList {
		obj, err := getResource(resource.Kind, resource.ApiVersion, resource.Namespace, resource.Name)
		if err != nil {
			return nil, err
		}
		result, err := getJSONPath(resource.Path, obj)
		if err != nil {
			return nil, err
		}
		for _, element := range result {
			var image string
			if versionRegex.MatchString(element) {
				tmp, err := getImageFromVersion(element)
				if err != nil {
					return nil, err
				}
				image = tmp
			} else if strings.Contains(element, "@") || strings.Contains(element, ":") {
				image = element
			} else {
				return nil, fmt.Errorf("Format error. %s is not a valid image/version", element)
			}
			version, info, err := getVersionInfoFromImage(image, reg)
			if err != nil {
				return nil, err
			}
			versionMap[version] = info
		}
	}
	return versionMap, nil
}

func FindSRM(a []srov1beta1.SpecialResourceModule, x string) (int, bool) {
	for i, n := range a {
		if x == n.GetName() {
			return i, true
		}
	}
	return -1, false
}

// SpecialResourceModuleReconciler reconciles a SpecialResource object
type SpecialResourceModuleReconciler struct {
	Log     logr.Logger
	Scheme  *runtime.Scheme
	reg     registry.Registry
	filter  filter.Filter
	watcher watcher.Watcher
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
	logModule = r.Log.WithName(color.Print("reconcile: "+req.Name, color.Purple))
	logModule.Info("Reconciling")

	srm := &srov1beta1.SpecialResourceModuleList{}

	opts := []client.ListOption{}
	err := clients.Interface.List(context.Background(), srm, opts...)
	if err != nil {
		return reconcile.Result{}, err
	}

	var request int
	var found bool
	//TODO check for deletion timestamp.
	if request, found = FindSRM(srm.Items, req.Name); !found {
		logModule.Info("Not found")
		return reconcile.Result{}, nil
	}
	resource := srm.Items[request]
	logModule.Info("Resource", "resource", resource)

	if !resource.Status.ImageStreamCreated {
		logModule.Info("ImageStream not found. Creating")
		if err := createImageStream(req.Name, resource.Spec.Namespace); err != nil {
			return reconcile.Result{}, err
		}
		resource.Status.ImageStreamCreated = true
	}

	if err := r.watcher.ReconcileWatches(resource); err != nil {
		logModule.Error(err, "failed to update watched resources")
		return reconcile.Result{}, err
	}

	//TODO cache images, wont change dynamically.
	clusterVersions, err := getOCPVersions(resource.Spec.Watch, r.reg)
	if err != nil {
		return reconcile.Result{}, err
	}

	logModule.Info("Checking versions in status")
	for resourceVersion, _ := range resource.Status.Versions {
		if _, ok := clusterVersions[resourceVersion]; !ok {
			//TODO not found. Need to remove everything
		} else {
			//TODO Found, need to check for reconcile stage.
		}
	}
	logModule.Info("Checking versions from the cluster")
	for clusterVersion, _ := range clusterVersions {
		if _, ok := resource.Status.Versions[clusterVersion]; !ok {
			//TODO not found, this version is new. reconcile.
		}
	}
	//TODO update resource status.
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
			Owns(&imagev1.ImageStream{}).
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
