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

package hash_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1beta1"
	"github.com/aws/karpenter-core/pkg/test"
	. "github.com/aws/karpenter-core/pkg/test/expectations"
)

var _ = Describe("NodePool Static Drift Hash", func() {
	var nodePool *v1beta1.NodePool
	BeforeEach(func() {
		nodePool = test.NodePool(v1beta1.NodePool{
			Spec: v1beta1.NodePoolSpec{
				Template: v1beta1.NodeClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"keyAnnotation":  "valueAnnotation",
							"keyAnnotation2": "valueAnnotation2",
						},
						Labels: map[string]string{
							"keyLabel": "valueLabel",
						},
					},
					Spec: v1beta1.NodeClaimSpec{
						Taints: []v1.Taint{
							{
								Key:    "key",
								Effect: v1.TaintEffectNoExecute,
							},
						},
						StartupTaints: []v1.Taint{
							{
								Key:    "key",
								Effect: v1.TaintEffectNoExecute,
							},
						},
						KubeletConfiguration: &v1beta1.Kubelet{
							MaxPods: ptr.Int32(10),
						},
					},
				},
			},
		})
	})
	It("should update the static drift hash when NodePool static field is updated", func() {
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		expectedHash := nodePool.Hash()
		Expect(nodePool.Annotations).To(HaveKeyWithValue(v1beta1.NodePoolHashAnnotationKey, expectedHash))

		nodePool.Spec.Template.Labels = map[string]string{"keyLabeltest": "valueLabeltest"}
		nodePool.Spec.Template.Annotations = map[string]string{"keyAnnotation2": "valueAnnotation2", "keyAnnotation": "valueAnnotation"}
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		expectedHashTwo := nodePool.Hash()
		Expect(nodePool.Annotations).To(HaveKeyWithValue(v1beta1.NodePoolHashAnnotationKey, expectedHashTwo))
	})
	It("should not update the static drift hash when NodePool behavior field is updated", func() {
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		expectedHash := nodePool.Hash()
		Expect(nodePool.Annotations).To(HaveKeyWithValue(v1beta1.NodePoolHashAnnotationKey, expectedHash))

		nodePool.Spec.Limits = v1beta1.Limits(v1.ResourceList{"cpu": resource.MustParse("16")})
		nodePool.Spec.Disruption.ConsolidationPolicy = v1beta1.ConsolidationPolicyWhenEmpty
		nodePool.Spec.Disruption.ConsolidateAfter = &v1beta1.NillableDuration{Duration: lo.ToPtr(30 * time.Second)}
		nodePool.Spec.Disruption.ExpireAfter.Duration = lo.ToPtr(30 * time.Second)
		nodePool.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirement{
			{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test"}},
			{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"1"}},
			{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"1"}},
		}
		nodePool.Spec.Weight = lo.ToPtr(int32(80))
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		Expect(nodePool.Annotations).To(HaveKeyWithValue(v1beta1.NodePoolHashAnnotationKey, expectedHash))
	})
})