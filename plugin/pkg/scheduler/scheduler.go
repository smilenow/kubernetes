/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

package scheduler

// Note: if you change code in this file, you might need to change code in
// contrib/mesos/pkg/scheduler/.

import (
	"time"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/record"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/plugin/pkg/scheduler/metrics"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"

	"github.com/golang/glog"
	"math/rand"
)

// Binder knows how to write a binding.
type Binder interface {
	Bind(binding *api.Binding) error
}

// Scheduler watches for new unscheduled pods. It attempts to find
// nodes that they fit on and writes bindings back to the api server.
type Scheduler struct {
	config *Config
}

type Config struct {
	// It is expected that changes made via SchedulerCache will be observed
	// by NodeLister and Algorithm.
	SchedulerCache schedulercache.Cache
	NodeLister     algorithm.NodeLister
	Algorithm      algorithm.ScheduleAlgorithm
	Binder         Binder

	// NextPod should be a function that blocks until the next pod
	// is available. We don't use a channel for this, because scheduling
	// a pod may take some amount of time and we don't want pods to get
	// stale while they sit in a channel.
	NextPod func() *api.Pod

	// Error is called if there is an error. It is passed the pod in
	// question, and the error
	Error func(*api.Pod, error)

	// Recorder is the EventRecorder to use
	Recorder record.EventRecorder

	// Close this to shut down the scheduler.
	StopEverything chan struct{}
}

// New returns a new scheduler.
func New(c *Config) *Scheduler {
	s := &Scheduler{
		config: c,
	}
	metrics.Register()
	return s
}

// Run begins watching and scheduling. It starts a goroutine and returns immediately.
func (s *Scheduler) Run() {
//	go wait.Until(s.scheduleOne, 0, s.config.StopEverything)
	go wait.Until(s.scheduleOne_relaxed_randomization, 0, s.config.StopEverything)
//	go wait.Until(s.scheduleBatch_relaxed_randomization, 0, s.config.StopEverything)
}

func (s *Scheduler) scheduleOne() {
	pod := s.config.NextPod()

	glog.V(3).Infof("Attempting to schedule: %+v", pod)
	start := time.Now()
	dest, err := s.config.Algorithm.Schedule(pod, s.config.NodeLister)
	if err != nil {
		glog.V(1).Infof("Failed to schedule: %+v", pod)
		s.config.Recorder.Eventf(pod, api.EventTypeWarning, "FailedScheduling", "%v", err)
		s.config.Error(pod, err)
		return
	}
	metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInMicroseconds(start))

	b := &api.Binding{
		ObjectMeta: api.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
		Target: api.ObjectReference{
			Kind: "Node",
			Name: dest,
		},
	}

	bindAction := func() bool {
		bindingStart := time.Now()
		err := s.config.Binder.Bind(b)
		if err != nil {
			glog.V(1).Infof("Failed to bind pod: %+v", err)
			s.config.Recorder.Eventf(pod, api.EventTypeNormal, "FailedScheduling", "Binding rejected: %v", err)
			s.config.Error(pod, err)
			return false
		}
		metrics.BindingLatency.Observe(metrics.SinceInMicroseconds(bindingStart))
		s.config.Recorder.Eventf(pod, api.EventTypeNormal, "Scheduled", "Successfully assigned %v to %v", pod.Name, dest)
		return true
	}

	assumed := *pod
	assumed.Spec.NodeName = dest
	// We want to assume the pod if and only if the bind succeeds,
	// but we don't want to race with any deletions, which happen asynchronously.
	s.config.SchedulerCache.AssumePodIfBindSucceed(&assumed, bindAction)

	metrics.E2eSchedulingLatency.Observe(metrics.SinceInMicroseconds(start))
}

var relaxedRandomizationOnePod	= 2
var relaxedRandomizationBatch	= 6	// 2 for each pod, and three pods as a batch

func (s *Scheduler) scheduleOne_relaxed_randomization() {
	pod := s.config.NextPod()

	glog.V(3).Infof("Attempting to schedule: %+v", pod)
	start := time.Now()

	nodes, err := s.config.NodeLister.List()
	if err != nil {
		glog.V(1).Infof("Failed to schedule: %+v", pod)
		s.config.Recorder.Eventf(pod, api.EventTypeWarning, "FailedScheduling", "%v", err)
		s.config.Error(pod, err)
		return
	}
	exists := map[int]bool{}
	randomNum := make([]int, relaxedRandomizationOnePod)
	randomNodes := []api.Node{}
	if len(nodes.Items) > relaxedRandomizationOnePod {
		for i := 0; i < relaxedRandomizationOnePod; i++ {
			randomNum[i] = scheduleRandomOne(len(nodes.Items), exists)
			exists[randomNum[i]] = true
			randomNodes = append(randomNodes, nodes.Items[randomNum[i]])
		}
	} else {
		randomNodes = nodes.Items
	}
	randomNodeList := api.NodeList{Items: randomNodes}

	dest, err := s.config.Algorithm.Schedule(pod, algorithm.FakeNodeLister(randomNodeList))

	if err != nil {
		glog.V(1).Infof("Failed to schedule: %+v", pod)
		s.config.Recorder.Eventf(pod, api.EventTypeWarning, "FailedScheduling", "%v", err)
		s.config.Error(pod, err)
		return
	}
	metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInMicroseconds(start))

	b := &api.Binding{
		ObjectMeta: api.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
		Target: api.ObjectReference{
			Kind: "Node",
			Name: dest,
		},
	}

	bindAction := func() bool {
		bindingStart := time.Now()
		err := s.config.Binder.Bind(b)
		if err != nil {
			glog.V(1).Infof("Failed to bind pod: %+v", err)
			s.config.Recorder.Eventf(pod, api.EventTypeNormal, "FailedScheduling", "Binding rejected: %v", err)
			s.config.Error(pod, err)
			return false
		}
		metrics.BindingLatency.Observe(metrics.SinceInMicroseconds(bindingStart))
		s.config.Recorder.Eventf(pod, api.EventTypeNormal, "Scheduled", "Successfully assigned %v to %v", pod.Name, dest)
		return true
	}

	assumed := *pod
	assumed.Spec.NodeName = dest
	// We want to assume the pod if and only if the bind succeeds,
	// but we don't want to race with any deletions, which happen asynchronously.
	s.config.SchedulerCache.AssumePodIfBindSucceed(&assumed, bindAction)

	metrics.E2eSchedulingLatency.Observe(metrics.SinceInMicroseconds(start))
}

func (s *Scheduler) scheduleBatch_relaxed_randomization() {
	nodes, err := s.config.NodeLister.List()
	if err != nil {
		return
	}
	randomNodes := []api.Node{}
	if len(nodes.Items) > relaxedRandomizationBatch {
		randomNum := schedulerRandomBatch(len(nodes.Items), relaxedRandomizationBatch)
		for _, i := range randomNum {
			randomNodes = append(randomNodes, nodes.Items[i])
		}
	} else {
		randomNodes = nodes.Items
	}
	randomNodeList := api.NodeList{Items: randomNodes}

	for batch := 0; batch < 3; batch++ {
		pod := s.config.NextPod()

		glog.V(3).Infof("Attempting to schedule: %+v", pod)
		start := time.Now()
		//dest, err := s.config.Algorithm.Schedule(pod, s.config.NodeLister)
		dest, err := s.config.Algorithm.Schedule(pod, algorithm.FakeNodeLister(randomNodeList))
		if err != nil {
			glog.V(1).Infof("Failed to schedule: %+v", pod)
			s.config.Recorder.Eventf(pod, api.EventTypeWarning, "FailedScheduling", "%v", err)
			s.config.Error(pod, err)
			return
		}
		metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInMicroseconds(start))

		b := &api.Binding{
			ObjectMeta: api.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
			Target: api.ObjectReference{
				Kind: "Node",
				Name: dest,
			},
		}

		bindAction := func() bool {
			bindingStart := time.Now()
			err := s.config.Binder.Bind(b)
			if err != nil {
				glog.V(1).Infof("Failed to bind pod: %+v", err)
				s.config.Recorder.Eventf(pod, api.EventTypeNormal, "FailedScheduling", "Binding rejected: %v", err)
				s.config.Error(pod, err)
				return false
			}
			metrics.BindingLatency.Observe(metrics.SinceInMicroseconds(bindingStart))
			s.config.Recorder.Eventf(pod, api.EventTypeNormal, "Scheduled", "Successfully assigned %v to %v", pod.Name, dest)
			return true
		}

		assumed := *pod
		assumed.Spec.NodeName = dest
		// We want to assume the pod if and only if the bind succeeds,
		// but we don't want to race with any deletions, which happen asynchronously.
		s.config.SchedulerCache.AssumePodIfBindSucceed(&assumed, bindAction)

		metrics.E2eSchedulingLatency.Observe(metrics.SinceInMicroseconds(start))
	}
}

func scheduleRandomOne(n int, exists map[int]bool) (int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		ret := r.Intn(n)
		if _, ok := exists[ret]; !ok {
			return ret
		}
	}
	return 0
}

func schedulerRandomBatch(n, m int) ([]int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	cur := make([]int, n)
	for i := 0; i < n; i++ {
		cur[i] = i
	}
	ret := make([]int, m)
	randN := n;
	for i := 0; i < m; i++ {
		randNow := r.Intn(randN)
		ret[i] = cur[randNow]
		randN--
		cur[randNow] = cur[randN]
	}
	return ret
}