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

package events

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/events"
)

func EvictPod(pod *v1.Pod) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           v1.EventTypeNormal,
		Reason:         "Evicted",
		Message:        "Evicted pod",
		DedupeValues:   []string{pod.Name},
	}
}

func DeletePod(pod *v1.Pod) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           v1.EventTypeNormal,
		Reason:         "Deleted",
		Message:        fmt.Sprintf("Deleted pod regardless of PDBs and lifecycle hooks, %v seconds before node termination to accommodate its terminationGracePeriodSeconds", pod.Spec.TerminationGracePeriodSeconds),
		DedupeValues:   []string{pod.Name},
	}
}

func NodeFailedToDrain(node *v1.Node, err error) events.Event {
	return events.Event{
		InvolvedObject: node,
		Type:           v1.EventTypeWarning,
		Reason:         "FailedDraining",
		Message:        fmt.Sprintf("Failed to drain node, %s", err),
		DedupeValues:   []string{node.Name},
	}
}

func NodeTerminationGracePeriod(node *v1.Node, expirationTime string) events.Event {
	return events.Event{
		InvolvedObject: node,
		Type:           v1.EventTypeWarning,
		Reason:         "TerminationGracePeriodExpiration",
		Message:        fmt.Sprintf("Node will have the out-of-service taint applied at: %s", expirationTime),
		DedupeValues:   []string{node.Name},
	}
}
