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

package termination_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clock "k8s.io/utils/clock/testing"
	. "knative.dev/pkg/logging/testing"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodeclaimtermination "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/termination"

	"sigs.k8s.io/karpenter/pkg/apis"
	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	nodeclaimlifecycle "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/controller"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/operator/scheme"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"

	"sigs.k8s.io/karpenter/pkg/test"
)

var ctx context.Context
var env *test.Environment
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var nodeClaimLifecycleController controller.Controller
var nodeClaimTerminationController controller.Controller

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Termination")
}

var _ = BeforeSuite(func() {
	fakeClock = clock.NewFakeClock(time.Now())
	env = test.NewEnvironment(scheme.Scheme, test.WithCRDs(apis.CRDs...), test.WithFieldIndexers(func(c cache.Cache) error {
		return c.IndexField(ctx, &v1.Node{}, "spec.providerID", func(obj client.Object) []string {
			return []string{obj.(*v1.Node).Spec.ProviderID}
		})
	}))
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	nodeClaimLifecycleController = nodeclaimlifecycle.NewController(fakeClock, env.Client, cloudProvider, events.NewRecorder(&record.FakeRecorder{}))
	nodeClaimTerminationController = nodeclaimtermination.NewController(env.Client, cloudProvider)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	fakeClock.SetTime(time.Now())
	ExpectCleanedUp(ctx, env.Client)
	cloudProvider.Reset()
})

var _ = Describe("Termination", func() {
	var nodePool *v1beta1.NodePool
	var nodeClaim *v1beta1.NodeClaim

	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClaim = test.NodeClaim(v1beta1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1beta1.NodePoolLabelKey: nodePool.Name,
				},
				Finalizers: []string{
					v1beta1.TerminationFinalizer,
				},
			},
			Spec: v1beta1.NodeClaimSpec{
				Resources: v1beta1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:          resource.MustParse("2"),
						v1.ResourceMemory:       resource.MustParse("50Mi"),
						v1.ResourcePods:         resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
	})
	It("should delete the node and the CloudProvider NodeClaim when NodeClaim deletion is triggered", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimLifecycleController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node)

		// Expect the node and the nodeClaim to both be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectFinalizersRemoved(ctx, env.Client, node)
		ExpectNotFound(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // now all nodes are gone so nodeClaim deletion continues
		ExpectNotFound(ctx, env.Client, nodeClaim, node)

		// Expect the nodeClaim to be gone from the cloudprovider
		_, err = cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(cloudprovider.IsNodeClaimNotFoundError(err)).To(BeTrue())
	})
	It("should delete multiple Nodes if multiple Nodes map to the NodeClaim", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimLifecycleController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node1 := test.NodeClaimLinkedNode(nodeClaim)
		node2 := test.NodeClaimLinkedNode(nodeClaim)
		node3 := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node1, node2, node3)

		// Expect the node and the nodeClaim to both be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectFinalizersRemoved(ctx, env.Client, node1, node2, node3)
		ExpectNotFound(ctx, env.Client, node1, node2, node3)

		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // now all nodes are gone so nodeClaim deletion continues
		ExpectNotFound(ctx, env.Client, nodeClaim, node1, node2, node3)

		// Expect the nodeClaim to be gone from the cloudprovider
		_, err = cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(cloudprovider.IsNodeClaimNotFoundError(err)).To(BeTrue())
	})
	It("should not delete the NodeClaim until all the Nodes are removed", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimLifecycleController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node)

		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // the node still hasn't been deleted, so the nodeClaim should remain

		ExpectExists(ctx, env.Client, nodeClaim)
		ExpectExists(ctx, env.Client, node)

		ExpectFinalizersRemoved(ctx, env.Client, node)
		ExpectNotFound(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // now the nodeClaim should be gone

		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should not call Delete() on the CloudProvider if the NodeClaim hasn't been launched yet", func() {
		nodeClaim.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)

		// Expect the nodeClaim to be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim))

		Expect(cloudProvider.DeleteCalls).To(HaveLen(0))
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should not delete nodes without provider ids if the NodeClaim hasn't been launched yet", func() {
		// Generate 10 nodes, none of which have a provider id
		var nodes []*v1.Node
		for i := 0; i < 10; i++ {
			nodes = append(nodes, test.Node())
		}
		ExpectApplied(ctx, env.Client, lo.Map(nodes, func(n *v1.Node, _ int) client.Object { return n })...)

		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)

		// Expect the nodeClaim to be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim))

		ExpectNotFound(ctx, env.Client, nodeClaim)
		for _, node := range nodes {
			ExpectExists(ctx, env.Client, node)
		}
	})
	It("should retry nodeClaim deletion when cloudProvider returns retryable error", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimLifecycleController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node)

		// Expect the node and the nodeClaim to both be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectFinalizersRemoved(ctx, env.Client, node)
		ExpectNotFound(ctx, env.Client, node)

		cloudProvider.NextDeleteErr = cloudprovider.NewRetryableError(fmt.Errorf("underlying instance not terminated"))
		result := ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim))
		Expect(result.RequeueAfter).To(Equal(time.Second * 10))

		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) //re-enqueue reconciliation since we got retryable error previously
		ExpectNotFound(ctx, env.Client, nodeClaim, node)

		// Expect the nodeClaim to be gone from the cloudprovider
		_, err = cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(cloudprovider.IsNodeClaimNotFoundError(err)).To(BeTrue())
	})
})
