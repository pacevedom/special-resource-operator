package resourcehelper

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	notUpdateableResources = map[string]bool{
		"ServiceAccount": true,
		"Pod":            true,
	}

	notNamespacedResources = map[string]bool{
		"Namespace":                 true,
		"ClusterRole":               true,
		"ClusterRoleBinding":        true,
		"SecurityContextConstraint": true,
		"SpecialResource":           true,
	}

	resourcesNeedingVersionUpdated = map[string]bool{
		"SecurityContextConstraints":     true,
		"Service":                        true,
		"ServiceMonitor":                 true,
		"Route":                          true,
		"Build":                          true,
		"BuildRun":                       true,
		"BuildConfig":                    true,
		"ImageStream":                    true,
		"PrometheusRule":                 true,
		"CSIDriver":                      true,
		"Issuer":                         true,
		"CustomResourceDefinition":       true,
		"Certificate":                    true,
		"SpecialResource":                true,
		"OperatorGroup":                  true,
		"CertManager":                    true,
		"MutatingWebhookConfiguration":   true,
		"ValidatingWebhookConfiguration": true,
		"Deployment":                     true,
		"ImagePolicy":                    true,
	}
)

//go:generate mockgen -source=helper.go -package=resourcehelper -destination=mock_helper_api.go

type Helper interface {
	IsNamespaced(kind string) bool
	IsNotUpdateable(kind string) bool
	NeedsResourceVersionUpdate(kind string) bool
	UpdateResourceVersion(req *unstructured.Unstructured, found *unstructured.Unstructured) error
	SetNodeSelectorTerms(obj *unstructured.Unstructured, terms map[string]string) error
	IsOneTimer(obj *unstructured.Unstructured) (bool, error)
	SetLabel(obj *unstructured.Unstructured, label string) error
	SetMetaData(obj *unstructured.Unstructured, nm string, ns string)
}

func New() Helper {
	return &resourceHelper{}
}

type resourceHelper struct{}

func (rh *resourceHelper) IsNamespaced(kind string) bool {
	return !notNamespacedResources[kind]
}

func (rh *resourceHelper) IsNotUpdateable(kind string) bool {
	// ServiceAccounts cannot be updated, maybe delete and create?
	return notUpdateableResources[kind]
}

// Some resources need an updated resourceversion, during updates
func (rh *resourceHelper) NeedsResourceVersionUpdate(kind string) bool {
	return resourcesNeedingVersionUpdated[kind]
}

func (rh *resourceHelper) UpdateResourceVersion(req *unstructured.Unstructured, found *unstructured.Unstructured) error {

	kind := found.GetKind()

	if rh.NeedsResourceVersionUpdate(kind) {
		version, fnd, err := unstructured.NestedString(found.Object, "metadata", "resourceVersion")
		if err != nil || !fnd {
			return fmt.Errorf("error or resourceVersion not found: %w", err)
		}

		if err = unstructured.SetNestedField(req.Object, version, "metadata", "resourceVersion"); err != nil {
			return fmt.Errorf("couldn't update ResourceVersion: %w", err)
		}

	}

	if kind == "Service" {
		clusterIP, fnd, err := unstructured.NestedString(found.Object, "spec", "clusterIP")
		if err != nil || !fnd {
			return fmt.Errorf("error or clusterIP not found: %w", err)
		}

		if err = unstructured.SetNestedField(req.Object, clusterIP, "spec", "clusterIP"); err != nil {
			return fmt.Errorf("couldn't update clusterIP: %w", err)
		}
	}

	return nil
}

func (rh *resourceHelper) SetNodeSelectorTerms(obj *unstructured.Unstructured, terms map[string]string) error {
	switch obj.GetKind() {
	case "DaemonSet", "Deployment", "StatefulSet":
		if err := rh.nodeSelectorTerms(terms, obj, "spec", "template", "spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"); err != nil {
			return fmt.Errorf("cannot setup %s nodeSelector: %w", obj.GetKind(), err)
		}

	case "Pod":
		if err := rh.nodeSelectorTerms(terms, obj, "spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"); err != nil {
			return fmt.Errorf("cannot setup %s nodeSelector: %w", obj.GetKind(), err)
		}
	}

	return nil
}

func NodeAffinityNames(nodeNames []string, obj *unstructured.Unstructured, fields ...string) error {
	if len(nodeNames) == 0 {
		return nil
	}
	nodeSelectorTerms, found, err := unstructured.NestedSlice(obj.Object, fields...)
	if err != nil {
		return err
	}
	if !found {
		nodeSelectorTerms = []interface{}{map[string]interface{}{}}
	}

	expression := make(map[string]interface{})
	expression["key"] = "kubernetes.io/hostname"
	expression["operator"] = "In"
	valueList := make([]interface{}, len(nodeNames))
	for i, name := range nodeNames {
		valueList[i] = name
	}
	expression["values"] = valueList

	for i := range nodeSelectorTerms {
		match := nodeSelectorTerms[i].(map[string]interface{})
		matchExpressions, found, err := unstructured.NestedSlice(match, "matchExpressions")
		if err != nil {
			return err
		}
		if !found {
			matchExpressions = make([]interface{}, 0)
		}
		matchExpressions = append(matchExpressions, expression)
		if err := unstructured.SetNestedSlice(match, matchExpressions, "matchExpressions"); err != nil {
			return err
		}
	}

	if err = unstructured.SetNestedSlice(obj.Object, nodeSelectorTerms, fields...); err != nil {
		return fmt.Errorf("cannot update nodeSelector for %s : %w", obj.GetName(), err)
	}

	return nil
}

func (rh *resourceHelper) nodeSelectorTerms(terms map[string]string, obj *unstructured.Unstructured, fields ...string) error {
	if len(terms) == 0 {
		return nil
	}

	nodeSelectorTerms, found, err := unstructured.NestedSlice(obj.Object, fields...)
	if err != nil {
		return err
	}
	if !found {
		nodeSelectorTerms = []interface{}{map[string]interface{}{}}
	}

	expressions := make([]interface{}, 0)
	for k, v := range terms {
		expression := make(map[string]interface{})
		expression["key"] = k
		expression["operator"] = "In"
		expression["values"] = []interface{}{v}
		expressions = append(expressions, expression)
	}

	for i := range nodeSelectorTerms {
		match := nodeSelectorTerms[i].(map[string]interface{})
		matchExpressions, found, err := unstructured.NestedSlice(match, "matchExpressions")
		if err != nil {
			return err
		}
		if !found {
			matchExpressions = make([]interface{}, 0)
		}
		matchExpressions = append(matchExpressions, expressions...)
		if err := unstructured.SetNestedSlice(match, matchExpressions, "matchExpressions"); err != nil {
			return err
		}
	}

	if err = unstructured.SetNestedSlice(obj.Object, nodeSelectorTerms, fields...); err != nil {
		return fmt.Errorf("cannot update nodeSelector for %s : %w", obj.GetName(), err)
	}

	return nil
}

func (rh *resourceHelper) IsOneTimer(obj *unstructured.Unstructured) (bool, error) {

	// We are not recreating Pods that have restartPolicy: Never
	if obj.GetKind() == "Pod" {
		restartPolicy, found, err := unstructured.NestedString(obj.Object, "spec", "restartPolicy")
		if err != nil || !found {
			return false, fmt.Errorf("error or restartPolicy not found: %w", err)
		}

		if restartPolicy == "Never" {
			return true, nil
		}
	}

	return false, nil
}

func (rh *resourceHelper) SetLabel(obj *unstructured.Unstructured, label string) error {

	var labels map[string]string

	if labels = obj.GetLabels(); labels == nil {
		labels = make(map[string]string)
	}

	labels[label] = "true"
	obj.SetLabels(labels)

	return rh.setSubResourceLabel(obj, label)
}

func (rh *resourceHelper) setSubResourceLabel(obj *unstructured.Unstructured, label string) error {

	switch obj.GetKind() {
	case "DaemonSet", "Deployment", "StatefulSet":
		labels, found, err := unstructured.NestedMap(obj.Object, "spec", "template", "metadata", "labels")
		if err != nil {
			return err
		}
		if !found {
			return errors.New("labels not found")
		}

		labels[label] = "true"
		if err := unstructured.SetNestedMap(obj.Object, labels, "spec", "template", "metadata", "labels"); err != nil {
			return err
		}

		// TODO: how to set label ownership for Builds and related Pods
		/*
			case "BuildConfig":
				output, found, err := unstructured.NestedMap(obj.Object, "spec", "output")
				if err != nil {
					return err
				}
				if !found {
					return errors.New("output not found")
				}

				labels := make(map[string]interface{})
				labels["name"] = filter.OwnedLabel
				labels["value"] = "true"
				imageLabels := append(make([]interface{}, 0), labels)

				if _, found := output["imageLabels"]; !found {
					err := unstructured.SetNestedSlice(obj.Object, imageLabels, "spec", "output", "imageLabels")
					if err != nil {
						return err
					}
				}
		*/
	}

	return nil
}

func (rh *resourceHelper) SetMetaData(obj *unstructured.Unstructured, nm string, ns string) {

	annotations := obj.GetAnnotations()

	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations["meta.helm.sh/release-name"] = nm
	annotations["meta.helm.sh/release-namespace"] = ns

	obj.SetAnnotations(annotations)

	labels := obj.GetLabels()

	if labels == nil {
		labels = make(map[string]string)
	}

	labels["app.kubernetes.io/managed-by"] = "Helm"

	obj.SetLabels(labels)
}
