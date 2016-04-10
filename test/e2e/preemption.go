package e2e

import (
	"strconv"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/util"
	"k8s.io/kubernetes/pkg/api/unversioned"

	client "k8s.io/kubernetes/pkg/client/unversioned"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"k8s.io/kubernetes/pkg/labels"
	"fmt"
	"time"
)

const testPriorityPreemption = "test-priority-preemption"

var _ = KubeDescribe("Graduation Project Preemption", func() {
	framework := NewDefaultFramework("pods")

	var c *client.Client
	var ns string

	BeforeEach(func(){
		c = framework.Client
		ns = framework.Namespace.Name
	})

	It("should be preemptive created according to its priority [Conformance]", func() {
		podsClient := c.Pods(ns)
		podNumber := 100

		var pods []*api.Pod
		pods = make([]*api.Pod, podNumber)

		By(fmt.Sprintf("create the pods with prioirty between 1 to %v", podNumber))
		for i := 1; i <= podNumber; i++ {
			prioirty := new(int64)
			*prioirty = int64(i)
			pods[i-1] = createTestPodWithPreemption(prioirty)
		}

		defer func() {
			By("deleting the pods")
			for i := 0; i < podNumber; i++ {
				err := podsClient.Delete(pods[i].Name, api.NewDeleteOptions(0))
				if err != nil {
					Failf("Failed to delete pod: %v", err)
				}
			}
		}()

		By("submitting the pods to kubernetes")
		for i := 0; i < podNumber; i++ {
			currentPod, err := podsClient.Create(pods[i])
			pods[i] = currentPod
			Logf("Pod priority: %v", i+1)
			Logf("Pod create time: %v", time.Now().UnixNano())
			if err != nil {
				Failf("Failed to create pod: %v", err)
			}
		}

		By("waiting for all pods scheduled")
		selector := labels.SelectorFromSet(labels.Set(map[string]string{"name": testPriorityPreemption}))
		_, err := waitForPodsWithLabelScheduled(c, ns, selector)
		expectNoError(err, "Error waiting for %d pods to be scheduled - probably a timeout", podNumber)

		By("waiting for all pods running")
		selector = labels.SelectorFromSet(labels.Set(map[string]string{"name": testPriorityPreemption}))
		err = waitForPodsWithLabelRunning(c, ns, selector)
		expectNoError(err, "Error waiting for %d pods to be running - probably a timeout", podNumber)

		By("verifying the pods are in kubernetes")
		selector = labels.SelectorFromSet(labels.Set(map[string]string{"name": testPriorityPreemption}))
		options := api.ListOptions{LabelSelector: selector}
		podList, err := podsClient.List(options)
		if err != nil {
			Failf("Failed to List pods: %v", err)
		}
		Expect(len(podList.Items)).To(Equal(podNumber))

		Logf("Print Pods status")
		for i := 0; i < len(podList.Items); i++ {
			currentPod := podList.Items[i]
			Logf("Pod prioirty: %v", *currentPod.Spec.Priority)
			Logf("Pod start time: %v", currentPod.Status.StartTime.UnixNano())
		}
		Logf("Finish")
	})
})

func createTestPodWithPreemption(priority *int64) *api.Pod {
	podName := "pod-with-preemption-" + strconv.FormatInt(*priority, 10) + "--" + string(util.NewUUID())

	return &api.Pod{
		TypeMeta: unversioned.TypeMeta {
			APIVersion: "v1",
			Kind: "Pod",
		},
		ObjectMeta: api.ObjectMeta {
			Name: podName,
			Labels: map[string]string {
				"name":		testPriorityPreemption,
				"priority":	strconv.FormatInt(*priority, 10),
			},
		},
		Spec: api.PodSpec {
			Containers: []api.Container {
				{
					Name:		testPriorityPreemption,
					Image: "gcr.io/google_containers/pause:2.0",
					//Image:		"gcr.io/google_containers/busybox:1.24",
					//Command: 	[]string{"/bin/sh", "-c", "echo container is alive; sleep 120"},
				},
			},
			RestartPolicy: api.RestartPolicyNever,
			Priority: priority,
		},
	}
}