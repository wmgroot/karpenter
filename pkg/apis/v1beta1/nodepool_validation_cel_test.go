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

package v1beta1_test

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Pallinder/go-randomdata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/ptr"

	. "sigs.k8s.io/karpenter/pkg/apis/v1beta1"
)

var _ = Describe("CEL/Validation", func() {
	var nodePool *NodePool

	BeforeEach(func() {
		if env.Version.Minor() < 25 {
			Skip("CEL Validation is for 1.25>")
		}
		nodePool = &NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(randomdata.SillyName())},
			Spec: NodePoolSpec{
				Template: NodeClaimTemplate{
					Spec: NodeClaimSpec{
						NodeClassRef: &NodeClassReference{
							Kind: "NodeClaim",
							Name: "default",
						},
						Requirements: []NodeSelectorRequirementWithMinValues{
							{
								NodeSelectorRequirement: v1.NodeSelectorRequirement{
									Key:      CapacityTypeLabelKey,
									Operator: v1.NodeSelectorOpExists,
								},
							},
						},
					},
				},
			},
		}
	})
	Context("Disruption", func() {
		It("should fail on negative expireAfter", func() {
			nodePool.Spec.Disruption.ExpireAfter.Duration = lo.ToPtr(lo.Must(time.ParseDuration("-1s")))
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed on a disabled expireAfter", func() {
			nodePool.Spec.Disruption.ExpireAfter.Duration = nil
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed on a valid expireAfter", func() {
			nodePool.Spec.Disruption.ExpireAfter.Duration = lo.ToPtr(lo.Must(time.ParseDuration("30s")))
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail on negative consolidateAfter", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = &NillableDuration{Duration: lo.ToPtr(lo.Must(time.ParseDuration("-1s")))}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed on a disabled consolidateAfter", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = &NillableDuration{Duration: nil}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed on a valid consolidateAfter", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = &NillableDuration{Duration: lo.ToPtr(lo.Must(time.ParseDuration("30s")))}
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmpty
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when setting consolidateAfter with consolidationPolicy=WhenEmpty", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = &NillableDuration{Duration: lo.ToPtr(lo.Must(time.ParseDuration("30s")))}
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmpty
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail when setting consolidateAfter with consolidationPolicy=WhenUnderutilized", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = &NillableDuration{Duration: lo.ToPtr(lo.Must(time.ParseDuration("30s")))}
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenUnderutilized
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed when not setting consolidateAfter to 'Never' with consolidationPolicy=WhenUnderutilized", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = &NillableDuration{Duration: nil}
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenUnderutilized
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail when creating a budget with an invalid cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("*"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a schedule with less than 5 entries", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("* * * * "),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a negative duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("-20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a seconds duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("30s"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a negative value int", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "-10",
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a negative value percent", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "-10%",
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a value percent with more than 3 digits", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "1000%",
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a cron but no duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("* * * * *"),
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a duration but no cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed when creating a budget with both duration and cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when creating a budget with hours and minutes in duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("2h20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when creating a budget with neither duration nor cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "10",
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when creating a budget with special cased crons", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: ptr.String("@annually"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail when creating two budgets where one has an invalid crontab", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{
				{
					Nodes:    "10",
					Schedule: ptr.String("@annually"),
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				},
				{
					Nodes:    "10",
					Schedule: ptr.String("*"),
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating multiple budgets where one doesn't have both schedule and duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{
				{
					Nodes:    "10",
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				},
				{
					Nodes:    "10",
					Schedule: ptr.String("* * * * *"),
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				},
				{
					Nodes: "10",
				},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
	})
	Context("KubeletConfiguration", func() {
		It("should succeed on kubeReserved with valid keys", func() {
			nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
				KubeReserved: map[string]string{
					string(v1.ResourceCPU): "2",
				},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed on systemReserved with valid keys", func() {
			nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
				SystemReserved: map[string]string{
					string(v1.ResourceCPU): "2",
				},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail on kubeReserved with invalid keys", func() {
			nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
				KubeReserved: map[string]string{
					string(v1.ResourcePods): "2",
				},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail on systemReserved with invalid keys", func() {
			nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
				SystemReserved: map[string]string{
					string(v1.ResourcePods): "2",
				},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		Context("Eviction Signals", func() {
			Context("Eviction Hard", func() {
				It("should succeed on evictionHard with valid keys and values", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						EvictionHard: map[string]string{
							"memory.available":   "5%",
							"nodefs.available":   "10%",
							"nodefs.inodesFree":  "15%",
							"imagefs.available":  "5%",
							"imagefs.inodesFree": "5%",
							"pid.available":      "5%",
						},
					}
					Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				})
				It("should succeed on evictionHard with valid keys and values", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						EvictionHard: map[string]string{
							"memory.available":   "20Mi",
							"nodefs.available":   "34G",
							"nodefs.inodesFree":  "25M",
							"imagefs.available":  "20Gi",
							"imagefs.inodesFree": "39Gi",
							"pid.available":      "20G",
						},
					}
					Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				})
				It("should fail on evictionHard with invalid keys", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						EvictionHard: map[string]string{
							"memory": "5%",
						},
					}
					Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				})
				It("should fail on invalid formatted percentage value in evictionHard", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						EvictionHard: map[string]string{
							"memory.available": "5%3",
						},
					}
					Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				})
				It("should fail on invalid percentage value (too large) in evictionHard", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						EvictionHard: map[string]string{
							"memory.available": "110%",
						},
					}
					Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				})
				It("should fail on invalid quantity value in evictionHard", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						EvictionHard: map[string]string{
							"memory.available": "110GB",
						},
					}
					Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				})
			})
		})
		Context("Eviction Soft", func() {
			It("should succeed on evictionSoft with valid keys and values", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available":   "5%",
						"nodefs.available":   "10%",
						"nodefs.inodesFree":  "15%",
						"imagefs.available":  "5%",
						"imagefs.inodesFree": "5%",
						"pid.available":      "5%",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available":   {Duration: time.Minute},
						"nodefs.available":   {Duration: time.Second * 90},
						"nodefs.inodesFree":  {Duration: time.Minute * 5},
						"imagefs.available":  {Duration: time.Hour},
						"imagefs.inodesFree": {Duration: time.Hour * 24},
						"pid.available":      {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			})
			It("should succeed on evictionSoft with valid keys and values", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available":   "20Mi",
						"nodefs.available":   "34G",
						"nodefs.inodesFree":  "25M",
						"imagefs.available":  "20Gi",
						"imagefs.inodesFree": "39Gi",
						"pid.available":      "20G",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available":   {Duration: time.Minute},
						"nodefs.available":   {Duration: time.Second * 90},
						"nodefs.inodesFree":  {Duration: time.Minute * 5},
						"imagefs.available":  {Duration: time.Hour},
						"imagefs.inodesFree": {Duration: time.Hour * 24},
						"pid.available":      {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			})
			It("should fail on evictionSoft with invalid keys", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory": "5%",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory": {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
			It("should fail on invalid formatted percentage value in evictionSoft", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available": "5%3",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available": {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
			It("should fail on invalid percentage value (too large) in evictionSoft", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available": "110%",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available": {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
			It("should fail on invalid quantity value in evictionSoft", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available": "110GB",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available": {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
			It("should fail when eviction soft doesn't have matching grace period", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available": "200Mi",
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
		})
		Context("GCThresholdPercent", func() {
			Context("ImageGCHighThresholdPercent", func() {
				It("should succeed on a imageGCHighThresholdPercent", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						ImageGCHighThresholdPercent: ptr.Int32(10),
					}
					Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				})
				It("should fail when imageGCHighThresholdPercent is less than imageGCLowThresholdPercent", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						ImageGCHighThresholdPercent: ptr.Int32(50),
						ImageGCLowThresholdPercent:  ptr.Int32(60),
					}
					Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				})
			})
			Context("ImageGCLowThresholdPercent", func() {
				It("should succeed on a imageGCLowThresholdPercent", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						ImageGCLowThresholdPercent: ptr.Int32(10),
					}
					Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				})
				It("should fail when imageGCLowThresholdPercent is greather than imageGCHighThresheldPercent", func() {
					nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
						ImageGCHighThresholdPercent: ptr.Int32(50),
						ImageGCLowThresholdPercent:  ptr.Int32(60),
					}
					Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				})
			})
		})
		Context("Eviction Soft Grace Period", func() {
			It("should succeed on evictionSoftGracePeriod with valid keys", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoft: map[string]string{
						"memory.available":   "5%",
						"nodefs.available":   "10%",
						"nodefs.inodesFree":  "15%",
						"imagefs.available":  "5%",
						"imagefs.inodesFree": "5%",
						"pid.available":      "5%",
					},
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available":   {Duration: time.Minute},
						"nodefs.available":   {Duration: time.Second * 90},
						"nodefs.inodesFree":  {Duration: time.Minute * 5},
						"imagefs.available":  {Duration: time.Hour},
						"imagefs.inodesFree": {Duration: time.Hour * 24},
						"pid.available":      {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			})
			It("should fail on evictionSoftGracePeriod with invalid keys", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory": {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
			It("should fail when eviction soft grace period doesn't have matching threshold", func() {
				nodePool.Spec.Template.Spec.Kubelet = &KubeletConfiguration{
					EvictionSoftGracePeriod: map[string]metav1.Duration{
						"memory.available": {Duration: time.Minute},
					},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			})
		})
	})
	Context("Taints", func() {
		It("should succeed for valid taints", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{
				{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule},
				{Key: "c", Value: "d", Effect: v1.TaintEffectNoExecute},
				{Key: "e", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "Test", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "test.com/Test", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "test.com.com/test", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "key-only", Effect: v1.TaintEffectNoExecute},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for invalid taint keys", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "test.com.com}", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "Test.com/test", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "test/test/test", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "test/", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "/test", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail at runtime for taint keys that are too long", func() {
			oldNodePool := nodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: fmt.Sprintf("test.com.test.%s/test", strings.ToLower(randomdata.Alphanumeric(250))), Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool = oldNodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: fmt.Sprintf("test.com.test/test-%s", strings.ToLower(randomdata.Alphanumeric(250))), Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for missing taint key", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid taint value", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "invalid-value", Effect: v1.TaintEffectNoSchedule, Value: "???"}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid taint effect", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "invalid-effect", Effect: "???"}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should not fail for same key with different effects", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{
				{Key: "a", Effect: v1.TaintEffectNoSchedule},
				{Key: "a", Effect: v1.TaintEffectNoExecute},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
	})
	Context("Requirements", func() {
		It("should succeed for valid requirement keys", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "Test", Operator: v1.NodeSelectorOpExists}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test.com/Test", Operator: v1.NodeSelectorOpExists}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test.com.com/test", Operator: v1.NodeSelectorOpExists}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "key-only", Operator: v1.NodeSelectorOpExists}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for invalid requirement keys", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test.com.com}", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "Test.com/test", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test/test/test", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test/", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "/test", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail at runtime for requirement keys that are too long", func() {
			oldNodePool := nodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: fmt.Sprintf("test.com.test.%s/test", strings.ToLower(randomdata.Alphanumeric(250))), Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool = oldNodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: fmt.Sprintf("test.com.test/test-%s", strings.ToLower(randomdata.Alphanumeric(250))), Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for the karpenter.sh/nodepool label", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: NodePoolLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{randomdata.SillyName()}}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should allow supported ops", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"1"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"1"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpExists}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for unsupported ops", func() {
			for _, op := range []v1.NodeSelectorOperator{"unknown"} {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: op, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should fail for restricted domains", func() {
			for label := range RestrictedLabelDomains {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: label + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should allow restricted domains exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range LabelDomainExceptions {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: label + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow restricted subdomains exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range LabelDomainExceptions {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "subdomain." + label + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow well known label exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range WellKnownLabels.Difference(sets.New(NodePoolLabelKey)) {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: label, Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow non-empty set after removing overlapped value", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test", "foo"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test", "bar"}}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should allow empty requirements", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail with invalid GT or LT values", func() {
			for _, requirement := range []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"1", "2"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"a"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"-1"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"1", "2"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"a"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"-1"}}},
			} {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{requirement}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should error when minValues is negative", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"insance-type-1"}}, MinValues: lo.ToPtr(-1)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should error when minValues is zero", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"insance-type-1"}}, MinValues: lo.ToPtr(0)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should error when minValues is more than 50", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpExists}, MinValues: lo.ToPtr(51)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should allow more than 50 values if minValues is not specified.", func() {
			var instanceTypes []string
			for i := 0; i < 90; i++ {
				instanceTypes = append(instanceTypes, "instance"+strconv.Itoa(i))
			}
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: instanceTypes}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should error when minValues is greater than the number of unique values specified within In operator", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"insance-type-1"}}, MinValues: lo.ToPtr(2)},
			}
			Expect(nodePool.Validate(ctx)).ToNot(Succeed())
		})
	})
	Context("Labels", func() {
		It("should allow unrecognized labels", func() {
			nodePool.Spec.Template.Labels = map[string]string{"foo": randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for the karpenter.sh/nodepool label", func() {
			nodePool.Spec.Template.Labels = map[string]string{NodePoolLabelKey: randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid label keys", func() {
			nodePool.Spec.Template.Labels = map[string]string{"spaces are not allowed": randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail at runtime for label keys that are too long", func() {
			oldNodePool := nodePool.DeepCopy()
			nodePool.Spec.Template.Labels = map[string]string{fmt.Sprintf("test.com.test.%s/test", strings.ToLower(randomdata.Alphanumeric(250))): randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool = oldNodePool.DeepCopy()
			nodePool.Spec.Template.Labels = map[string]string{fmt.Sprintf("test.com.test/test-%s", strings.ToLower(randomdata.Alphanumeric(250))): randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid label values", func() {
			nodePool.Spec.Template.Labels = map[string]string{randomdata.SillyName(): "/ is not allowed"}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for restricted label domains", func() {
			for label := range RestrictedLabelDomains {
				fmt.Println(label)
				nodePool.Spec.Template.Labels = map[string]string{label + "/unknown": randomdata.SillyName()}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should allow labels kOps require", func() {
			nodePool.Spec.Template.Labels = map[string]string{
				"kops.k8s.io/instancegroup": "karpenter-nodes",
				"kops.k8s.io/gpu":           "1",
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should allow labels in restricted domains exceptions list", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range LabelDomainExceptions {
				fmt.Println(label)
				nodePool.Spec.Template.Labels = map[string]string{
					label: "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow labels prefixed with the restricted domain exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range LabelDomainExceptions {
				nodePool.Spec.Template.Labels = map[string]string{
					fmt.Sprintf("%s/key", label): "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow subdomain labels in restricted domains exceptions list", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range LabelDomainExceptions {
				nodePool.Spec.Template.Labels = map[string]string{
					fmt.Sprintf("subdomain.%s", label): "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow subdomain labels prefixed with the restricted domain exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range LabelDomainExceptions {
				nodePool.Spec.Template.Labels = map[string]string{
					fmt.Sprintf("subdomain.%s/key", label): "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
	})
	Context("Resources", func() {
		It("should not allow resources to be set", func() {
			nodePool.Spec.Template.Spec.Resources = ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
	})
})
