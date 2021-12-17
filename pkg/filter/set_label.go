package filter

import (
	"errors"

	"github.com/openshift-psap/special-resource-operator/pkg/color"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	owned = "specialresource.openshift.io/owned"
)

var (
	log = zap.New(zap.UseDevMode(true)).WithName(color.Print("filter", color.Purple))
)

func SetLabel(obj *unstructured.Unstructured) error {

	var labels map[string]string

	if labels = obj.GetLabels(); labels == nil {
		labels = make(map[string]string)
	}

	// TODO: Parametrize label to apply (if this function gets used by an alternative controller (for SpecialResourceModule))
	labels[owned] = "true"
	obj.SetLabels(labels)

	return setSubResourceLabel(obj)
}

func setSubResourceLabel(obj *unstructured.Unstructured) error {

	if obj.GetKind() == "DaemonSet" || obj.GetKind() == "Deployment" ||
		obj.GetKind() == "StatefulSet" {

		labels, found, err := unstructured.NestedMap(obj.Object, "spec", "template", "metadata", "labels")
		if err != nil {
			return err
		}
		if !found {
			return errors.New("Labels not found")
		}

		labels[owned] = "true"
		if err := unstructured.SetNestedMap(obj.Object, labels, "spec", "template", "metadata", "labels"); err != nil {
			return err
		}
	}

	if obj.GetKind() == "BuildConfig" {
		log.Info("TODO: how to set label ownership for Builds and related Pods")
		/*
			output, found, err := unstructured.NestedMap(obj.Object, "spec", "output")
			if err != nil {
				return err
			}
			if !found {
				return errors.New("output not found")
			}

			label := make(map[string]interface{})
			label["name"] = owned
			label["value"] = "true"
			imageLabels := append(make([]interface{}, 0), label)

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
