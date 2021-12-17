package filter_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/openshift-psap/special-resource-operator/pkg/filter"
	buildv1 "github.com/openshift/api/build/v1"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ownedLabel = "specialresource.openshift.io/owned"
)

var _ = Describe("SetLabel", func() {
	objs := []runtime.Object{
		&v1.DaemonSet{
			TypeMeta: metav1.TypeMeta{Kind: "DaemonSet"},
		},
		&v1.Deployment{
			TypeMeta: metav1.TypeMeta{Kind: "Deployment"},
		},
		&v1.StatefulSet{
			TypeMeta: metav1.TypeMeta{Kind: "StatefulSet"},
		},
	}

	entries := make([]TableEntry, 0, len(objs))

	for _, o := range objs {
		entries = append(entries, Entry(o.GetObjectKind().GroupVersionKind().Kind, o))
	}

	testFunc := func(o client.Object) {
		mo, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		Expect(err).NotTo(HaveOccurred())

		uo := unstructured.Unstructured{Object: mo}

		// Create the map manually, otherwise SetLabel returns an error
		err = unstructured.SetNestedStringMap(uo.Object, map[string]string{}, "spec", "template", "metadata", "labels")
		Expect(err).NotTo(HaveOccurred())

		err = filter.SetLabel(&uo)
		Expect(err).NotTo(HaveOccurred())

		Expect(uo.GetLabels()).To(HaveKeyWithValue(ownedLabel, "true"))

		v, found, err := unstructured.NestedString(
			uo.Object,
			"spec",
			"template",
			"metadata",
			"labels",
			ownedLabel)

		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(v).To(Equal("true"))
	}

	DescribeTable("should the label", testFunc, entries...)

	It("should the label for BuildConfig", func() {
		bc := buildv1.BuildConfig{
			TypeMeta: metav1.TypeMeta{Kind: "BuildConfig"},
		}

		mo, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&bc)
		Expect(err).NotTo(HaveOccurred())

		uo := unstructured.Unstructured{Object: mo}

		err = filter.SetLabel(&uo)
		Expect(err).NotTo(HaveOccurred())
		Expect(uo.GetLabels()).To(HaveKeyWithValue(ownedLabel, "true"))
	})
})
