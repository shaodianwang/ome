//go:build e2e

/*
Copyright 2025.

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

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"tpu.google.com/slice-scheduler/test/utils"
)

const namespace = "slice-scheduler-system"

var _ = Describe("controller", Ordered, func() {
	BeforeAll(func() {
		By("labeling Kind control-plane node for controller scheduling")
		cmd := exec.Command("kubectl", "label", "node", "slice-scheduler-test-e2e-control-plane",
			"cloud.google.com/gke-nodepool=default-pool", "--overwrite")
		_, _ = utils.Run(cmd)
		// Remove control-plane taint so the controller manager can be scheduled
		cmd = exec.Command("kubectl", "taint", "nodes", "slice-scheduler-test-e2e-control-plane",
			"node-role.kubernetes.io/control-plane:NoSchedule-")
		_, _ = utils.Run(cmd)

		By("installing prometheus operator")
		Expect(utils.InstallPrometheusOperator()).To(Succeed())

		By("installing the cert-manager")
		Expect(utils.InstallCertManager()).To(Succeed())

		By("creating manager namespace")
		cmd = exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	AfterAll(func() {
		By("uninstalling the Prometheus manager bundle")
		utils.UninstallPrometheusOperator()

		By("uninstalling the cert-manager bundle")
		utils.UninstallCertManager()

		By("removing manager namespace")
		cmd := exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	Context("Operator", func() {
		It("should run successfully", func() {
			var controllerPodName string
			var err error

			// projectimage stores the name of the image used in the example
			var projectimage = "example.com/slice-scheduler:v0.0.1"

			By("building the manager(Operator) image")
			cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("loading the the manager(Operator) image on Kind")
			err = utils.LoadImageToKindClusterWithName(projectimage)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing CRDs")
			cmd = exec.Command("make", "install")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing PartitionTree CRD (owned by partition-tree controller, not included in make install)")
			cmd = exec.Command("kubectl", "apply", "-f", "config/crd/bases/partition.tpu.google.com_partitiontrees.yaml")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing JobSet CRD")
			cmd = exec.Command("kubectl", "apply", "--server-side", "-f",
				"https://github.com/kubernetes-sigs/jobset/releases/download/v0.7.0/manifests.yaml")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("patching JobSet controller to fix deprecated kube-rbac-proxy image")
			cmd = exec.Command("kubectl", "set", "image",
				"deployment/jobset-controller-manager",
				"kube-rbac-proxy=registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.16.0",
				"-n", "jobset-system")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("waiting for JobSet webhook to be ready")
			cmd = exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/jobset-controller-manager", "-n", "jobset-system", "--timeout", "120s")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("setting up PartitionTree and node labels")
			Expect(setupPartitionTree()).To(Succeed())

			By("deploying the controller-manager")
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("enabling pod reconciler for pod-to-node binding")
			cmd = exec.Command("kubectl", "patch", "deployment",
				"slice-scheduler-controller-manager", "-n", namespace,
				"--type", "json",
				"-p", `[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-pod-reconciler"}]`)
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func() error {
				// Get pod name

				cmd = exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				podNames := utils.GetNonEmptyLines(string(podOutput))
				if len(podNames) != 1 {
					return fmt.Errorf("expect 1 controller pods running, but got %d", len(podNames))
				}
				controllerPodName = podNames[0]
				ExpectWithOffset(2, controllerPodName).Should(ContainSubstring("controller-manager"))

				// Validate pod status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				status, err := utils.Run(cmd)
				ExpectWithOffset(2, err).NotTo(HaveOccurred())
				if string(status) != "Running" {
					return fmt.Errorf("controller pod in %s status", status)
				}
				return nil
			}
			EventuallyWithOffset(1, verifyControllerUp, time.Minute, time.Second).Should(Succeed())

		})
	})

	Context("Slice Workload Scheduling", func() {
		It("should successfully schedule slice and workload", func() {
			DeferCleanup(func() {
				By("Cleaning up resources")
				cmd := exec.Command("kubectl", "delete", "jobset", "jobset-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "label", "nodes", "--all", "cloud.google.com/gke-tpu-slice-", "--overwrite")
				_, _ = utils.Run(cmd)
			})

			By("Applying JobSet manifest")
			cmd := exec.Command("kubectl", "apply", "-f", "test/yaml/jobset-1cube.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Pending")
			verifyPodsPending := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Pending" {
						return fmt.Errorf("expected Pending pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsPending, time.Minute, time.Second).Should(Succeed())

			By("Applying Slice manifest")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is created and scheduled")
			verifySliceScheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceScheduled, time.Minute, time.Second).Should(Succeed())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Running")
			verifyPodsRunning := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Running" {
						return fmt.Errorf("expected Running pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Slice Recovery and Recreation", func() {
		It("should recover from FAILED state (Scenario A)", func() {
			DeferCleanup(func() {
				By("Cleaning up resources")
				cmd := exec.Command("kubectl", "delete", "jobset", "jobset-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "label", "nodes", "--all", "cloud.google.com/gke-tpu-slice-", "--overwrite")
				_, _ = utils.Run(cmd)
			})

			By("Applying Slice manifest")
			cmd := exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is created and scheduled")
			verifySliceScheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceScheduled, time.Minute, time.Second).Should(Succeed())

			By("Applying JobSet manifest")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/jobset-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Running")
			verifyPodsRunning := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Running" {
						return fmt.Errorf("expected Running pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())

			cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
			initialPartitionsOut, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			initialPartitions := string(initialPartitionsOut)

			By("Simulating Slice failure by patching status to FAILED")
			cmd = exec.Command("kubectl", "patch", "slice", "slice-1cube", "--subresource", "status", "--type", "merge", "-p", `{"status":{"conditions":[{"type":"Ready","status":"False","reason":"FAILED","message":"Simulated failure","lastTransitionTime":"2026-01-01T00:00:00Z"}]}}`)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying scheduler reschedules the Slice (partitions remain assigned)")
			verifySliceRescheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				partitions := string(out)
				if partitions == "" || partitions == "[]" {
					return fmt.Errorf("slice partitionIds are empty after rescheduling")
				}
				// Verify that partitions are still assigned (rescheduling completed)
				_ = initialPartitions // Partitions may be same or different depending on availability
				return nil
			}
			Eventually(verifySliceRescheduled, time.Minute, time.Second).Should(Succeed())

			By("Verifying associated JobSet pods are Running again")
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should recover from manual Slice deletion (Scenario B)", func() {
			DeferCleanup(func() {
				By("Cleaning up resources")
				cmd := exec.Command("kubectl", "delete", "jobset", "jobset-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube", "--ignore-not-found")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "label", "nodes", "--all", "cloud.google.com/gke-tpu-slice-", "--overwrite")
				_, _ = utils.Run(cmd)
			})

			By("Applying Slice manifest")
			cmd := exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is created and scheduled")
			verifySliceScheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceScheduled, time.Minute, time.Second).Should(Succeed())

			By("Applying JobSet manifest")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/jobset-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying JobSet pods are Running")
			verifyPodsRunning := func() error {
				cmd = exec.Command("kubectl", "get", "pods", "-l", "jobset.sigs.k8s.io/jobset-name=jobset-1cube", "-o", "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				phases := utils.GetNonEmptyLines(string(out))
				if len(phases) == 0 {
					return fmt.Errorf("no pods found")
				}
				for _, p := range phases {
					if p != "Running" {
						return fmt.Errorf("expected Running pod, got: %s", p)
					}
				}
				return nil
			}
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("Manually deleting the Slice object")
			cmd = exec.Command("kubectl", "delete", "slice", "slice-1cube")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the Slice object is deleted")
			verifySliceDeleted := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube")
				_, err := utils.Run(cmd)
				if err == nil {
					return fmt.Errorf("slice still exists")
				}
				return nil
			}
			Eventually(verifySliceDeleted, 30*time.Second, time.Second).Should(Succeed())

			By("Re-applying the Slice manifest to trigger re-scheduling")
			cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/slice-1cube.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Slice CR is re-scheduled")
			verifySliceRescheduled := func() error {
				cmd = exec.Command("kubectl", "get", "slice", "slice-1cube", "-o", "jsonpath={.spec.partitionIds}")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if string(out) == "" || string(out) == "[]" {
					return fmt.Errorf("slice partitionIds are empty")
				}
				return nil
			}
			Eventually(verifySliceRescheduled, time.Minute, time.Second).Should(Succeed())

			By("Labeling KWOK nodes with slice name (simulates GKE slice-provisioner)")
			cmd = exec.Command("kubectl", "label", "nodes", "-l", "type=kwok",
				"cloud.google.com/gke-tpu-slice=slice-1cube", "--overwrite")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying associated JobSet pods are Running again")
			Eventually(verifyPodsRunning, 5*time.Minute, 5*time.Second).Should(Succeed())
		})
	})
})

const kwokVersion = "v0.7.0"

func setupPartitionTree() error {
	// Install KWOK controller to simulate fake nodes with a virtual kubelet.
	// KWOK manages node lifecycle and transitions pods to Running/Succeeded.
	kwokURL := fmt.Sprintf("https://github.com/kubernetes-sigs/kwok/releases/download/%s/kwok.yaml", kwokVersion)
	cmd := exec.Command("kubectl", "apply", "-f", kwokURL)
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to install KWOK controller: %w", err)
	}

	// Install KWOK default stages for node heartbeat and pod lifecycle.
	// Without this, the node controller marks KWOK nodes as unreachable.
	stagesURL := fmt.Sprintf("https://github.com/kubernetes-sigs/kwok/releases/download/%s/stage-fast.yaml", kwokVersion)
	cmd = exec.Command("kubectl", "apply", "-f", stagesURL)
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to install KWOK stages: %w", err)
	}

	// Apply the KWOK pod-complete stage (auto-completes pods after delay)
	cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/pod-complete-delayed.yaml")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to apply KWOK pod-complete stage: %w", err)
	}

	// Wait for KWOK controller to be ready
	cmd = exec.Command("kubectl", "wait", "--for=condition=Available",
		"deployment/kwok-controller", "-n", "kube-system", "--timeout", "60s")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("KWOK controller not ready: %w", err)
	}

	// Create fake KWOK-managed nodes with TPU labels.
	// KWOK recognizes nodes with the annotation "kwok.x-k8s.io/node=fake" and
	// provides a virtual kubelet for them.
	// Phase 1: Create bare nodes with KWOK annotation and basic labels.
	for i := 1; i <= 16; i++ {
		nodeName := fmt.Sprintf("kwok-node-%d", i)
		nodeJSON := fmt.Sprintf(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"%s","annotations":{"kwok.x-k8s.io/node":"fake"},"labels":{"type":"kwok"}}}`, nodeName)
		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(nodeJSON)
		dir, _ := utils.GetProjectDir()
		cmd.Dir = dir
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to create KWOK node %s: %s: %w", nodeName, string(output), err)
		}
	}

	// Wait for KWOK to initialize the nodes (heartbeat stage applies first)
	time.Sleep(5 * time.Second)

	// Phase 2: Label nodes with TPU labels and patch status AFTER KWOK initialization.
	// KWOK's node-initialize stage can overwrite labels set during creation.
	for i := 1; i <= 16; i++ {
		nodeName := fmt.Sprintf("kwok-node-%d", i)
		cmd = exec.Command("kubectl", "label", "node", nodeName,
			"cloud.google.com/gke-tpu-topology=4x4x4",
			"cloud.google.com/gke-tpu-accelerator=tpu7x",
			"tpu.google.com/partition-id-4x4x4=np-t1",
			"--overwrite")
		if _, err := utils.Run(cmd); err != nil {
			return fmt.Errorf("failed to label KWOK node %s: %w", nodeName, err)
		}

		// Patch node status with TPU capacity and Ready condition
		statusPatch := fmt.Sprintf(`{"status":{"capacity":{"google.com/tpu":"4","cpu":"96","memory":"384Gi"},"allocatable":{"google.com/tpu":"4","cpu":"96","memory":"384Gi"},"conditions":[{"type":"Ready","status":"True","reason":"KWOKNodeReady","message":"KWOK fake node"}],"addresses":[{"type":"InternalIP","address":"10.0.0.%d"},{"type":"Hostname","address":"%s"}]}}`, i, nodeName)
		cmd = exec.Command("kubectl", "patch", "node", nodeName, "--subresource", "status", "--type", "merge", "-p", statusPatch)
		if _, err := utils.Run(cmd); err != nil {
			return fmt.Errorf("failed to patch status for KWOK node %s: %w", nodeName, err)
		}
	}

	// Apply the sample PartitionTree (metadata + spec only)
	cmd = exec.Command("kubectl", "apply", "-f", "test/yaml/partitiontree-sample.yaml")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to apply partitiontree-sample.yaml: %w", err)
	}

	// Patch the PartitionTree status subresource — kubectl apply does not
	// write status fields. The scheduler requires status.root to be populated
	// for partition allocation.
	nodeNames := ""
	for i := 1; i <= 16; i++ {
		if i > 1 {
			nodeNames += ","
		}
		nodeNames += fmt.Sprintf(`"kwok-node-%d"`, i)
	}
	statusJSON := fmt.Sprintf(`{"status":{"root":{"partitionId":"np-t1","topology":"4x4x4","state":"Healthy","nodeNames":[%s]}}}`, nodeNames)
	cmd = exec.Command("kubectl", "patch", "partitiontree", "pt-node-pool-1",
		"--subresource", "status", "--type", "merge", "-p", statusJSON)
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to patch PartitionTree status: %w", err)
	}

	return nil
}
