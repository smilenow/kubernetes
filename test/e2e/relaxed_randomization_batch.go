package e2e

import (
	"fmt"
	"time"
	"strconv"

	client "k8s.io/kubernetes/pkg/client/unversioned"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/util"
	"k8s.io/kubernetes/pkg/labels"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const testRelaxedRandomization = "test-relaxed-randomization"

var _ = KubeDescribe("Graduation Project Batch Relaxed Randomization", func(){
	framework := NewDefaultFramework("graduation-relaxed-randomization")
	var c *client.Client
	var ns string

	BeforeEach(func(){
		c = framework.Client
		ns = framework.Namespace.Name
	})

	It("should setup 60 nodes, and submit 1200 pods", func(){
		nodeNumber := 60
		podNumber := nodeNumber * 20

		pods := make([]*api.Pod,podNumber)

		By("creating the nodes")
		for i := 0; i < nodeNumber; i++ {
			node := createNode(i)
			nodeName := createNodeName(i)
			_, err := c.Nodes().Create(node)
			expectNoError(err, "Error creating for %v node", nodeName)
			Logf("creating node %v successfully", i)
		}
		Logf("created")
		defer func(){
			By("deleting the pods")
			for i := 0; i < podNumber; i ++ {
				err := c.Pods(ns).Delete(pods[i].Name, api.NewDeleteOptions(0))
				if err != nil {
					Failf("Failed to delete pod: %v", err)
				}
			}
			Logf("deleted")

			By("deleting the nodes")
			for i := 0; i < nodeNumber; i++ {
				nodeName := createNodeName(i)
				err := c.Nodes().Delete(nodeName)
				expectNoError(err, "Error deleting for %v node", nodeName)
				Logf("deleting node %v successfully", i)
			}
			Logf("deleted")
		}()

		By("checking the nodes number")
		nodes, err := c.Nodes().List(api.ListOptions{})
		if err != nil {
			Logf("unable to fetch node list: %v", err)
			return
		}
		Expect(len(nodes.Items)).To(Equal(nodeNumber+1))
		Logf("checked")

		By(fmt.Sprintf("submitting %v pods to kubernetes", podNumber))
		for i := 0; i < podNumber; i++ {
			pods[i], err = c.Pods(ns).Create(createTestPod())
			if err != nil {
				Failf("Failed to create %d pod: %v", i, err)
			}
		}

		By("sleep 3 mins")
		time.Sleep(3 * time.Minute)

		By("waiting for all pods scheduled")
		selector := labels.SelectorFromSet(labels.Set(map[string]string{"name": testRelaxedRandomization}))
		_, err = waitForPodsWithLabelScheduled(c, ns, selector)
		expectNoError(err, "Error waiting for %d pods to be scheduled - probably a timeout", podNumber)

		By("verifying the pods are in kubernetes")
		selector = labels.SelectorFromSet(labels.Set(map[string]string{"name": testRelaxedRandomization}))
		options := api.ListOptions{LabelSelector: selector}
		podList, err := c.Pods(ns).List(options)
		if err != nil {
			Failf("Failed to List pods: %v", err)
		}
		Expect(len(podList.Items)).To(Equal(podNumber))

		By("print Pods status")
		for i := 0; i < 20; i++ {
			Logf("Pod %d : %v \n", i, podList.Items[i])
		}
		Logf("printed")
	})

})

func createNodeName(i int) string {
	return "node-relaxed-randomization-test-node-" + strconv.Itoa(i)
}

func createNode(i int) *api.Node {
	nodeName := createNodeName(i)

	return &api.Node{
		ObjectMeta: api.ObjectMeta {
			Name: nodeName,
		},
		Spec: api.NodeSpec {
			Unschedulable: false,
		},
		Status: api.NodeStatus {
			Capacity: api.ResourceList {
				api.ResourcePods: *resource.NewQuantity(100, resource.DecimalSI), // 100 pods
				api.ResourceCPU: *resource.NewMilliQuantity(500, resource.DecimalSI), // 0.5 core
				api.ResourceMemory: *resource.NewQuantity(16106120, resource.BinarySI), // 0.015 GB
			},
			Allocatable: api.ResourceList {
				api.ResourcePods: *resource.NewQuantity(100, resource.DecimalSI),
				api.ResourceCPU: *resource.NewMilliQuantity(500, resource.DecimalSI),
				api.ResourceMemory: *resource.NewQuantity(16106120, resource.BinarySI),
			},
			Conditions: []api.NodeCondition{
				{
					Type: api.NodeReady,
					Status: api.ConditionTrue,
				},
			},
		},
	}
}


func createTestPod() *api.Pod {
	podName := "pod-relaxed-randomization-" + string(util.NewUUID())

	return &api.Pod{
		TypeMeta: unversioned.TypeMeta {
			APIVersion: "v1",
			Kind: "Pod",
		},
		ObjectMeta: api.ObjectMeta {
			Name: podName,
			Labels: map[string]string {
				"name": testRelaxedRandomization,
			},
		},
		Spec: api.PodSpec {
			Containers: []api.Container {
				{
					Name:	testRelaxedRandomization,
					Image:	"gcr.io/google_containers/pause:2.0",
					Resources: api.ResourceRequirements {
						Limits: api.ResourceList {
							api.ResourceCPU: *resource.NewMilliQuantity(5, resource.DecimalSI),
							api.ResourceMemory: *resource.NewQuantity(161062, resource.BinarySI),
						},
					},
				},
			},
			RestartPolicy: api.RestartPolicyNever,
		},
	}
}
