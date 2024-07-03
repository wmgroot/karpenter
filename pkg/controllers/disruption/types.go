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

package disruption

import (
	"bytes"
	"context"
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/pod"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/orchestration"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

const (
	GracefulDisruptionClass = "graceful" // graceful disruption always respects blocking pod PDBs and the do-not-disrupt annotation
	EventualDisruptionClass = "eventual" // eventual disruption is bounded by a NodePool's TerminationGracePeriod, regardless of blocking pod PDBs and the do-not-disrupt annotation
)

type Method interface {
	ShouldDisrupt(context.Context, *Candidate) bool
	ComputeCommand(context.Context, map[string]map[v1.DisruptionReason]int, ...*Candidate) (Command, scheduling.Results, error)
	Type() string
	Class() string
	ConsolidationType() string
}

type CandidateFilter func(context.Context, *Candidate) bool

// Candidate is a state.StateNode that we are considering for disruption along with extra information to be used in
// making that determination
type Candidate struct {
	*state.StateNode
	instanceType      *cloudprovider.InstanceType
	nodePool          *v1.NodePool
	zone              string
	capacityType      string
	disruptionCost    float64
	reschedulablePods []*corev1.Pod
}

//nolint:gocyclo
func NewCandidate(ctx context.Context, kubeClient client.Client, recorder events.Recorder, clk clock.Clock, node *state.StateNode, pdbs pdb.Limits,
	nodePoolMap map[string]*v1.NodePool, nodePoolToInstanceTypesMap map[string]map[string]*cloudprovider.InstanceType, queue *orchestration.Queue, disruptionClass string) (*Candidate, error) {
	var err error
	var pods []*corev1.Pod
	if err = node.ValidateNodeDisruptable(ctx, kubeClient); err != nil {
		recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, err.Error())...)
		return nil, err
	}
	// If the orchestration queue is already considering a candidate we want to disrupt, don't consider it a candidate.
	if queue.HasAny(node.ProviderID()) {
		return nil, fmt.Errorf("candidate is already being disrupted")
	}
	// We know that the node will have the label key because of the node.IsDisruptable check above
	nodePoolName := node.Labels()[v1.NodePoolLabelKey]
	nodePool := nodePoolMap[nodePoolName]
	instanceTypeMap := nodePoolToInstanceTypesMap[nodePoolName]
	// skip any candidates where we can't determine the nodePool
	if nodePool == nil || instanceTypeMap == nil {
		recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, fmt.Sprintf("NodePool %q not found", nodePoolName))...)
		return nil, fmt.Errorf("nodepool %q can't be resolved for state node", nodePoolName)
	}
	instanceType := instanceTypeMap[node.Labels()[corev1.LabelInstanceTypeStable]]
	// skip any candidates that we can't determine the instance of
	if instanceType == nil {
		recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, fmt.Sprintf("Instance Type %q not found", node.Labels()[corev1.LabelInstanceTypeStable]))...)
		return nil, fmt.Errorf("instance type %q can't be resolved", node.Labels()[corev1.LabelInstanceTypeStable])
	}
	if pods, err = node.ValidatePodsDisruptable(ctx, kubeClient, pdbs); err != nil {
		// if the disruption class is not eventual or the nodepool has no TerminationGracePeriod, block disruption of pods
		// if the error is anything but a PodBlockEvictionError, also block disruption of pods
		if !(state.IsPodBlockEvictionError(err) && node.NodeClaim.Spec.TerminationGracePeriod != nil && disruptionClass == EventualDisruptionClass) {
			recorder.Publish(disruptionevents.Blocked(node.Node, node.NodeClaim, err.Error())...)
			return nil, err
		}
	}
	return &Candidate{
		StateNode:         node.DeepCopy(),
		instanceType:      instanceType,
		nodePool:          nodePool,
		capacityType:      node.Labels()[v1.CapacityTypeLabelKey],
		zone:              node.Labels()[corev1.LabelTopologyZone],
		reschedulablePods: lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return pod.IsReschedulable(p) }),
		// We get the disruption cost from all pods in the candidate, not just the reschedulable pods
		disruptionCost: disruptionutils.ReschedulingCost(ctx, pods) * disruptionutils.LifetimeRemaining(clk, nodePool, node.NodeClaim),
	}, nil
}

type Command struct {
	candidates   []*Candidate
	replacements []*scheduling.NodeClaim
}

type Action string

var (
	NoOpAction    Action = "no-op"
	ReplaceAction Action = "replace"
	DeleteAction  Action = "delete"
)

func (c Command) Action() Action {
	switch {
	case len(c.candidates) > 0 && len(c.replacements) > 0:
		return ReplaceAction
	case len(c.candidates) > 0 && len(c.replacements) == 0:
		return DeleteAction
	default:
		return NoOpAction
	}
}

func (c Command) String() string {
	var buf bytes.Buffer
	podCount := lo.Reduce(c.candidates, func(_ int, cd *Candidate, _ int) int { return len(cd.reschedulablePods) }, 0)
	fmt.Fprintf(&buf, "%s, terminating %d nodes (%d pods) ", c.Action(), len(c.candidates), podCount)
	for i, old := range c.candidates {
		if i != 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s", old.Name())
		fmt.Fprintf(&buf, "/%s", old.instanceType.Name)
		fmt.Fprintf(&buf, "/%s", old.capacityType)
	}
	if len(c.replacements) == 0 {
		return buf.String()
	}
	odNodeClaims := 0
	spotNodeClaims := 0
	for _, nodeClaim := range c.replacements {
		ct := nodeClaim.Requirements.Get(v1.CapacityTypeLabelKey)
		if ct.Has(v1.CapacityTypeOnDemand) {
			odNodeClaims++
		}
		if ct.Has(v1.CapacityTypeSpot) {
			spotNodeClaims++
		}
	}
	// Print list of instance types for the first replacements.
	if len(c.replacements) > 1 {
		fmt.Fprintf(&buf, " and replacing with %d spot and %d on-demand, from types %s",
			spotNodeClaims, odNodeClaims,
			scheduling.InstanceTypeList(c.replacements[0].InstanceTypeOptions))
		return buf.String()
	}
	ct := c.replacements[0].Requirements.Get(v1.CapacityTypeLabelKey)
	nodeDesc := "node"
	if ct.Len() == 1 {
		nodeDesc = fmt.Sprintf("%s node", ct.Any())
	}
	fmt.Fprintf(&buf, " and replacing with %s from types %s",
		nodeDesc,
		scheduling.InstanceTypeList(c.replacements[0].InstanceTypeOptions))
	return buf.String()
}
