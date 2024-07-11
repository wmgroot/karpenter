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

package termination

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/karpenter/pkg/utils/termination"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/util/workqueue"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/operator/injection"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	terminatorevents "sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

// Controller for the resource
type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	terminator    *terminator.Terminator
	recorder      events.Recorder
}

// NewController constructs a controller instance
func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, terminator *terminator.Terminator, recorder events.Recorder) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		terminator:    terminator,
		recorder:      recorder,
	}
}

func (c *Controller) Reconcile(ctx context.Context, n *corev1.Node) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, "node.termination")

	if !n.GetDeletionTimestamp().IsZero() {
		return c.finalize(ctx, n)
	}
	return reconcile.Result{}, nil
}

//nolint:gocyclo
func (c *Controller) finalize(ctx context.Context, node *corev1.Node) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(node, v1.TerminationFinalizer) {
		return reconcile.Result{}, nil
	}
	if err := c.deleteAllNodeClaims(ctx, node); err != nil {
		return reconcile.Result{}, fmt.Errorf("deleting nodeclaims, %w", err)
	}

	nodeTerminationTime, err := c.nodeTerminationTime(node)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := c.terminator.Taint(ctx, node, v1.DisruptionNoScheduleTaint); err != nil {
		return reconcile.Result{}, fmt.Errorf("tainting node with %s, %w", v1.DisruptionTaintKey, err)
	}
	if err := c.terminator.Drain(ctx, node, nodeTerminationTime); err != nil {
		if !terminator.IsNodeDrainError(err) {
			return reconcile.Result{}, fmt.Errorf("draining node, %w", err)
		}
		c.recorder.Publish(terminatorevents.NodeFailedToDrain(node, err))
		// If the underlying NodeClaim no longer exists, we want to delete to avoid trying to gracefully draining
		// on nodes that are no longer alive. We do a check on the Ready condition of the node since, even
		// though the CloudProvider says the instance is not around, we know that the kubelet process is still running
		// if the Node Ready condition is true
		// Similar logic to: https://github.com/kubernetes/kubernetes/blob/3a75a8c8d9e6a1ebd98d8572132e675d4980f184/staging/src/k8s.io/cloud-provider/controllers/nodelifecycle/node_lifecycle_controller.go#L144
		if nodeutils.GetCondition(node, corev1.NodeReady).Status != corev1.ConditionTrue {
			if _, err := c.cloudProvider.Get(ctx, node.Spec.ProviderID); err != nil {
				if cloudprovider.IsNodeClaimNotFoundError(err) {
					return reconcile.Result{}, c.removeFinalizer(ctx, node)
				}
				return reconcile.Result{}, fmt.Errorf("getting nodeclaim, %w", err)
			}
		}

		return reconcile.Result{RequeueAfter: 1 * time.Second}, nil
	}
	nodeClaims, err := nodeutils.GetNodeClaims(ctx, node, c.kubeClient)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("deleting nodeclaims, %w", err)
	}
	for _, nodeClaim := range nodeClaims {
		isInstanceTerminated, err := termination.EnsureTerminated(ctx, c.kubeClient, nodeClaim, c.cloudProvider)
		if err != nil {
			// 404 = the nodeClaim no longer exists
			if errors.IsNotFound(err) {
				continue
			}
			// 409 - The nodeClaim exists, but its status has already been modified
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, fmt.Errorf("ensuring instance termination, %w", err)
		}
		if !isInstanceTerminated {
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	if err := c.removeFinalizer(ctx, node); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (c *Controller) deleteAllNodeClaims(ctx context.Context, node *corev1.Node) error {
	nodeClaims, err := nodeutils.GetNodeClaims(ctx, node, c.kubeClient)
	if err != nil {
		return err
	}
	for _, nodeClaim := range nodeClaims {
		// If we still get the NodeClaim, but it's already marked as terminating, we don't need to call Delete again
		if nodeClaim.DeletionTimestamp.IsZero() {
			if err := c.kubeClient.Delete(ctx, nodeClaim); err != nil {
				return client.IgnoreNotFound(err)
			}
		}
	}
	return nil
}

func (c *Controller) removeFinalizer(ctx context.Context, n *corev1.Node) error {
	stored := n.DeepCopy()
	controllerutil.RemoveFinalizer(n, v1.TerminationFinalizer)
	if !equality.Semantic.DeepEqual(stored, n) {
		if err := c.kubeClient.Patch(ctx, n, client.StrategicMergeFrom(stored)); err != nil {
			return client.IgnoreNotFound(fmt.Errorf("patching node, %w", err))
		}
		metrics.NodesTerminatedCounter.With(prometheus.Labels{
			metrics.NodePoolLabel: n.Labels[v1.NodePoolLabelKey],
		}).Inc()
		// We use stored.DeletionTimestamp since the api-server may give back a node after the patch without a deletionTimestamp
		TerminationSummary.With(prometheus.Labels{
			metrics.NodePoolLabel: n.Labels[v1.NodePoolLabelKey],
		}).Observe(time.Since(stored.DeletionTimestamp.Time).Seconds())
		log.FromContext(ctx).Info("deleted node")
	}
	return nil
}

func (c *Controller) nodeTerminationTime(node *corev1.Node) (*time.Time, error) {
	expirationTimeString, exists := node.ObjectMeta.Annotations[v1.NodeTerminationTimestampAnnotationKey]
	if !exists {
		return nil, nil
	}
	expirationTime, err := time.Parse(time.RFC3339, expirationTimeString)
	if err != nil {
		return nil, fmt.Errorf("parsing %s annotation, %w", v1.NodeTerminationTimestampAnnotationKey, err)
	}
	return &expirationTime, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("node.termination").
		For(&corev1.Node{}).
		WithOptions(
			controller.Options{
				RateLimiter: workqueue.NewMaxOfRateLimiter(
					workqueue.NewItemExponentialFailureRateLimiter(100*time.Millisecond, 10*time.Second),
					// 10 qps, 100 bucket size
					&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
				),
				MaxConcurrentReconciles: 100,
			},
		).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
