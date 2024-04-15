/*
Copyright The Kubernetes Authors.

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

package nodeclaim_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	. "knative.dev/pkg/logging/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"sigs.k8s.io/karpenter/pkg/apis"
	"sigs.k8s.io/karpenter/pkg/operator/scheme"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"

	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeclaimutil "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

var (
	ctx context.Context
	env *test.Environment
)

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodeClaimUtils")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(scheme.Scheme, test.WithCRDs(apis.CRDs...))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("NodeClaimUtils", func() {
	var node *v1.Node
	BeforeEach(func() {
		node = test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.LabelTopologyZone:            "test-zone-1",
					v1.LabelTopologyRegion:          "test-region",
					"test-label-key":                "test-label-value",
					"test-label-key2":               "test-label-value2",
					v1beta1.NodeRegisteredLabelKey:  "true",
					v1beta1.NodeInitializedLabelKey: "true",
					v1beta1.NodePoolLabelKey:        "default",
					v1beta1.CapacityTypeLabelKey:    v1beta1.CapacityTypeOnDemand,
					v1.LabelOSStable:                "linux",
					v1.LabelInstanceTypeStable:      "test-instance-type",
				},
				Annotations: map[string]string{
					"test-annotation-key":        "test-annotation-value",
					"test-annotation-key2":       "test-annotation-value2",
					"node-custom-annotation-key": "node-custom-annotation-value",
				},
			},
			ReadyStatus: v1.ConditionTrue,
			Taints: []v1.Taint{
				{
					Key:    "test-taint-key",
					Effect: v1.TaintEffectNoSchedule,
					Value:  "test-taint-value",
				},
				{
					Key:    "test-taint-key2",
					Effect: v1.TaintEffectNoExecute,
					Value:  "test-taint-value2",
				},
			},
			ProviderID: test.RandomProviderID(),
			Capacity: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("10"),
				v1.ResourceMemory:           resource.MustParse("10Mi"),
				v1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("8"),
				v1.ResourceMemory:           resource.MustParse("8Mi"),
				v1.ResourceEphemeralStorage: resource.MustParse("95Gi"),
			},
		})
	})
	It("should update the owner for a Node to a NodeClaim", func() {
		nodeClaim := test.NodeClaim(v1beta1.NodeClaim{
			Spec: v1beta1.NodeClaimSpec{
				NodeClassRef: &v1beta1.NodeClassReference{
					Kind:       "NodeClassRef",
					APIVersion: "test.cloudprovider/v1",
					Name:       "default",
				},
			},
		})
		node = test.Node(test.NodeOptions{ProviderID: nodeClaim.Status.ProviderID})
		node = nodeclaimutil.UpdateNodeOwnerReferences(nodeClaim, node)

		Expect(lo.Contains(node.OwnerReferences, metav1.OwnerReference{
			APIVersion:         lo.Must(apiutil.GVKForObject(nodeClaim, scheme.Scheme)).GroupVersion().String(),
			Kind:               lo.Must(apiutil.GVKForObject(nodeClaim, scheme.Scheme)).String(),
			Name:               nodeClaim.Name,
			UID:                nodeClaim.UID,
			BlockOwnerDeletion: lo.ToPtr(true),
		}))
	})
})
