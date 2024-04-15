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
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/samber/lo"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/controller"
	"sigs.k8s.io/karpenter/pkg/operator/scheme"
	"sigs.k8s.io/karpenter/pkg/test"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/ptr"

	. "knative.dev/pkg/logging/testing"

	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var ctx context.Context
var terminationController controller.Controller
var env *test.Environment
var defaultOwnerRefs = []metav1.OwnerReference{{Kind: "ReplicaSet", APIVersion: "appsv1", Name: "rs", UID: "1234567890"}}
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder
var queue *terminator.Queue

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Termination")
}

var _ = BeforeSuite(func() {
	fakeClock = clock.NewFakeClock(time.Now())
	env = test.NewEnvironment(scheme.Scheme, test.WithCRDs(apis.CRDs...), test.WithFieldIndexers(test.NodeClaimFieldIndexer(ctx)))

	cloudProvider = fake.NewCloudProvider()
	recorder = test.NewEventRecorder()
	queue = terminator.NewQueue(env.Client, recorder)
	terminationController = termination.NewController(env.Client, cloudProvider, terminator.NewTerminator(fakeClock, env.Client, queue), recorder)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Termination", func() {
	var node *v1.Node
	var nodeClaim *v1beta1.NodeClaim

	BeforeEach(func() {
		nodeClaim, node = test.NodeClaimAndNode(v1beta1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{v1beta1.TerminationFinalizer}}})
		node.Labels[v1beta1.NodePoolLabelKey] = test.NodePool().Name
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
	})

	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
		fakeClock.SetTime(time.Now())
		cloudProvider.Reset()
		queue.Reset()

		// Reset the metrics collectors
		metrics.NodesTerminatedCounter.Reset()
		termination.TerminationSummary.Reset()
		terminator.EvictionQueueDepth.Set(0)
	})

	Context("Reconciliation", func() {
		It("should delete nodes", func() {
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodeclaims associated with nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectExists(ctx, env.Client, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, node, nodeClaim)
		})
		It("should not race if deleting nodes in parallel", func() {
			var nodes []*v1.Node
			for i := 0; i < 10; i++ {
				node = test.Node(test.NodeOptions{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{v1beta1.TerminationFinalizer},
					},
				})
				ExpectApplied(ctx, env.Client, node)
				Expect(env.Client.Delete(ctx, node)).To(Succeed())
				node = ExpectNodeExists(ctx, env.Client, node.Name)
				nodes = append(nodes, node)
			}

			var wg sync.WaitGroup
			// this is enough to trip the race detector
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(node *v1.Node) {
					defer GinkgoRecover()
					defer wg.Done()
					ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
				}(nodes[i])
			}
			wg.Wait()
			ExpectNotFound(ctx, env.Client, lo.Map(nodes, func(n *v1.Node, _ int) client.Object { return n })...)
		})
		It("should exclude nodes from load balancers when terminating", func() {
			// This is a kludge to prevent the node from being deleted before we can
			// inspect its labels
			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Annotations:     map[string]string{v1beta1.DoNotDisruptAnnotationKey: "true"},
					OwnerReferences: defaultOwnerRefs,
				},
			})

			ExpectApplied(ctx, env.Client, node, podNoEvict)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Labels[v1.LabelNodeExcludeBalancers]).Should(Equal("karpenter"))
		})
		It("should not evict pods that tolerate karpenter disruption taint with equal operator", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podSkip := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []v1.Toleration{{Key: v1beta1.DisruptionTaintKey, Operator: v1.TolerationOpEqual, Effect: v1beta1.DisruptionNoScheduleTaint.Effect, Value: v1beta1.DisruptionNoScheduleTaint.Value}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, podEvict, podSkip)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			Expect(queue.Has(podSkip)).To(BeFalse())
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict pods that tolerate karpenter disruption taint with exists operator", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podSkip := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []v1.Toleration{{Key: v1beta1.DisruptionTaintKey, Operator: v1.TolerationOpExists, Effect: v1beta1.DisruptionNoScheduleTaint.Effect}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, podEvict, podSkip)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			Expect(queue.Has(podSkip)).To(BeFalse())
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			Expect(queue.Has(podSkip)).To(BeFalse())

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict pods that tolerate the node.kubernetes.io/unschedulable taint", func() {
			podEvict := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []v1.Toleration{{Key: v1.TaintNodeUnschedulable, Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, podEvict)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes that have pods without an ownerRef", func() {
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: nil,
				},
			})

			ExpectApplied(ctx, env.Client, node, pod)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect pod with no owner ref to be enqueued for eviction
			EventuallyExpectTerminating(ctx, env.Client, pod)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Delete no owner refs pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, pod)

			// Reconcile node to evict pod
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Reconcile to delete node
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes with terminal pods", func() {
			podEvictPhaseSucceeded := test.Pod(test.PodOptions{
				NodeName: node.Name,
				Phase:    v1.PodSucceeded,
			})
			podEvictPhaseFailed := test.Pod(test.PodOptions{
				NodeName: node.Name,
				Phase:    v1.PodFailed,
			})

			ExpectApplied(ctx, env.Client, node, podEvictPhaseSucceeded, podEvictPhaseFailed)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Trigger Termination Controller, which should ignore these pods and delete the node
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should fail to evict pods that violate a PDB", func() {
			minAvailable := intstr.FromInt32(1)
			labelSelector := map[string]string{test.RandomName(): test.RandomName()}
			pdb := test.PodDisruptionBudget(test.PDBOptions{
				Labels: labelSelector,
				// Don't let any pod evict
				MinAvailable: &minAvailable,
			})
			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Labels:          labelSelector,
					OwnerReferences: defaultOwnerRefs,
				},
				Phase: v1.PodRunning,
			})

			ExpectApplied(ctx, env.Client, node, podNoEvict, pdb)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podNoEvict to fail eviction due to PDB, and be retried
			Eventually(func() int {
				return queue.NumRequeues(terminator.NewQueueKey(podNoEvict))
			}).Should(BeNumerically(">=", 1))

			// Delete pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, podNoEvict)
			ExpectNotFound(ctx, env.Client, podNoEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict pods in order", func() {
			daemonEvict := test.DaemonSet()
			daemonNodeCritical := test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{PriorityClassName: "system-node-critical"}})
			daemonClusterCritical := test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{PriorityClassName: "system-cluster-critical"}})
			ExpectApplied(ctx, env.Client, node, daemonEvict, daemonNodeCritical, daemonClusterCritical)

			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podDaemonEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               daemonEvict.Name,
				UID:                daemonEvict.UID,
				Controller:         ptr.Bool(true),
				BlockOwnerDeletion: ptr.Bool(true),
			}}}})
			podNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podDaemonNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               daemonNodeCritical.Name,
				UID:                daemonNodeCritical.UID,
				Controller:         ptr.Bool(true),
				BlockOwnerDeletion: ptr.Bool(true),
			}}}})
			podDaemonClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               daemonClusterCritical.Name,
				UID:                daemonClusterCritical.UID,
				Controller:         ptr.Bool(true),
				BlockOwnerDeletion: ptr.Bool(true),
			}}}})

			ExpectApplied(ctx, env.Client, node, podEvict, podNodeCritical, podClusterCritical, podDaemonEvict, podDaemonNodeCritical, podDaemonClusterCritical)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectPodExists(ctx, env.Client, podEvict.Name, podEvict.Namespace)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Expect the noncritical Daemon pod to be evicted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectPodExists(ctx, env.Client, podDaemonEvict.Name, podDaemonEvict.Namespace)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			EventuallyExpectTerminating(ctx, env.Client, podDaemonEvict)
			ExpectDeleted(ctx, env.Client, podDaemonEvict)

			// Expect the critical pods to be evicted and deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectPodExists(ctx, env.Client, podNodeCritical.Name, podNodeCritical.Namespace)
			ExpectPodExists(ctx, env.Client, podClusterCritical.Name, podClusterCritical.Namespace)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			EventuallyExpectTerminating(ctx, env.Client, podNodeCritical, podClusterCritical)
			ExpectDeleted(ctx, env.Client, podNodeCritical)
			ExpectDeleted(ctx, env.Client, podClusterCritical)

			// Expect the critical daemon pods to be evicted and deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectPodExists(ctx, env.Client, podDaemonNodeCritical.Name, podDaemonNodeCritical.Namespace)
			ExpectPodExists(ctx, env.Client, podDaemonClusterCritical.Name, podDaemonClusterCritical.Namespace)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			EventuallyExpectTerminating(ctx, env.Client, podDaemonNodeCritical, podDaemonClusterCritical)
			ExpectDeleted(ctx, env.Client, podDaemonNodeCritical)
			ExpectDeleted(ctx, env.Client, podDaemonClusterCritical)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict non-critical pods first", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})

			ExpectApplied(ctx, env.Client, node, podEvict, podNodeCritical, podClusterCritical)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Expect the critical pods to be evicted and deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			EventuallyExpectTerminating(ctx, env.Client, podNodeCritical, podClusterCritical)
			ExpectDeleted(ctx, env.Client, podNodeCritical)
			ExpectDeleted(ctx, env.Client, podClusterCritical)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict static pods", func() {
			ExpectApplied(ctx, env.Client, node)
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, podEvict)

			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1",
						Kind:       "Node",
						Name:       node.Name,
						UID:        node.UID,
					}},
				},
			})
			ExpectApplied(ctx, env.Client, podNoEvict)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect mirror pod to not be queued for eviction
			Expect(queue.Has(podNoEvict)).To(BeFalse())

			// Expect podEvict to be enqueued for eviction then be successful
			EventuallyExpectTerminating(ctx, env.Client, podEvict)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Reconcile node to evict pod
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Delete pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)

		})
		It("should not delete nodes until all pods are deleted", func() {
			pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, pods[0], pods[1])

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect the pods to be evicted
			EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			ExpectDeleted(ctx, env.Client, pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			ExpectDeleted(ctx, env.Client, pods[0])

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes with no underlying instance even if not fully drained", func() {
			pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, pods[0], pods[1])

			// Make Node NotReady since it's automatically marked as Ready on first deploy
			ExpectMakeNodesNotReady(ctx, env.Client, node)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect the pods to be evicted
			EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// After this, the node still has one pod that is evicting.
			ExpectDeleted(ctx, env.Client, pods[1])

			// Remove the node from created nodeclaims so that the cloud provider returns DNE
			cloudProvider.CreatedNodeClaims = map[string]*v1beta1.NodeClaim{}

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not delete nodes with no underlying instance if the node is still Ready", func() {
			pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, pods[0], pods[1])

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			// Expect the pods to be evicted
			EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// After this, the node still has one pod that is evicting.
			ExpectDeleted(ctx, env.Client, pods[1])

			// Remove the node from created nodeclaims so that the cloud provider returns DNE
			cloudProvider.CreatedNodeClaims = map[string]*v1beta1.NodeClaim{}

			// Reconcile to try to delete the node, but don't succeed because the readiness condition
			// of the node still won't let us delete it
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeExists(ctx, env.Client, node.Name)
		})
		It("should wait for pods to terminate", func() {
			pod := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			fakeClock.SetTime(time.Now()) // make our fake clock match the pod creation time
			ExpectApplied(ctx, env.Client, node, pod)

			// Before grace period, node should not delete
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})
			ExpectNodeExists(ctx, env.Client, node.Name)
			EventuallyExpectTerminating(ctx, env.Client, pod)

			// After grace period, node should delete. The deletion timestamps are from etcd which we can't control, so
			// to eliminate test-flakiness we reset the time to current time + 90 seconds instead of just advancing
			// the clock by 90 seconds.
			fakeClock.SetTime(time.Now().Add(90 * time.Second))
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict a new pod with the same name using the old pod's eviction queue key", func() {
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pod",
					OwnerReferences: defaultOwnerRefs,
				},
			})
			ExpectApplied(ctx, env.Client, node, pod)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Don't trigger a call into the queue to make sure that we effectively aren't triggering eviction
			// We'll use this to try to leave pods in the queue

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Delete the pod directly to act like something else is doing the pod termination
			ExpectDeleted(ctx, env.Client, pod)

			// Requeue the termination controller to completely delete the node
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect that the old pod's key still exists in the queue
			Expect(queue.Has(pod)).To(BeTrue())

			// Re-create the pod and node, it should now have the same name, but a different UUID
			node = test.Node(test.NodeOptions{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{v1beta1.TerminationFinalizer},
				},
			})
			pod = test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pod",
					OwnerReferences: defaultOwnerRefs,
				},
			})
			ExpectApplied(ctx, env.Client, node, pod)

			// Trigger eviction queue with the pod key still in it
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{})

			Consistently(func(g Gomega) {
				g.Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
			}, ReconcilerPropagationTime, RequestInterval).Should(Succeed())
		})
	})
	Context("Metrics", func() {
		It("should fire the terminationSummary metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_termination_time_seconds", map[string]string{"nodepool": node.Labels[v1beta1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(m.GetSummary().GetSampleCount()).To(BeNumerically("==", 1))
		})
		It("should fire the nodesTerminated counter metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_terminated", map[string]string{"nodepool": node.Labels[v1beta1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(m.GetCounter().Value)).To(BeNumerically("==", 1))
		})
		It("should update the eviction queueDepth metric when reconciling pods", func() {
			minAvailable := intstr.FromInt32(0)
			labelSelector := map[string]string{test.RandomName(): test.RandomName()}
			pdb := test.PodDisruptionBudget(test.PDBOptions{
				Labels: labelSelector,
				// Don't let any pod evict
				MinAvailable: &minAvailable,
			})
			ExpectApplied(ctx, env.Client, pdb, node)
			pods := test.Pods(5, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: defaultOwnerRefs,
				Labels:          labelSelector,
			}})
			ExpectApplied(ctx, env.Client, lo.Map(pods, func(p *v1.Pod, _ int) client.Object { return p })...)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, queue, client.ObjectKey{}) // Reconcile the queue so that we set the metric

			ExpectMetricGaugeValue("karpenter_nodes_eviction_queue_depth", 5, map[string]string{})
		})
	})
})

func ExpectNodeWithNodeClaimDraining(c client.Client, nodeName string) *v1.Node {
	GinkgoHelper()
	node := ExpectNodeExists(ctx, c, nodeName)
	Expect(node.Spec.Taints).To(ContainElement(v1beta1.DisruptionNoScheduleTaint))
	Expect(lo.Contains(node.Finalizers, v1beta1.TerminationFinalizer)).To(BeTrue())
	Expect(node.DeletionTimestamp).ToNot(BeNil())
	return node
}
