package integ

import (
	"fmt"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func WaitUpdateAndValidate(c *C, s *IntegSuite, name string, updateFn func(app *v1beta1.FlinkApplication), failurePhase v1beta1.FlinkApplicationPhase) *v1beta1.FlinkApplication {

	// update with new appln image.
	app, err := s.Util.Update(name, updateFn)
	c.Assert(err, IsNil)

	for {
		// keep trying until the new job is launched
		newApp, err := s.Util.GetFlinkApplication(name)
		c.Assert(err, IsNil)
		if newApp.Status.JobStatus.JobID != "" &&
			newApp.Status.JobStatus.JobID != app.Status.JobStatus.JobID {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(s.Util.WaitForPhase(name, v1beta1.FlinkApplicationRunning, failurePhase), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(name), IsNil)

	// check that the new job started from an empty savepoint.
	newApp, _ := s.Util.GetFlinkApplication(name)
	c.Assert(newApp.Status.JobStatus.JobID, Not(Equals), app.Status.JobStatus.JobID)
	c.Assert(newApp.Status.SavepointPath, Equals, "")

	// wait for the old cluster to be cleaned up
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).List(v1.ListOptions{})
		c.Assert(err, IsNil)

		oldPodFound := false

		for _, pod := range pods.Items {
			if pod.Annotations["flink-app-hash"] == app.Status.DeployHash {
				oldPodFound = true
			}
		}

		if !oldPodFound {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return newApp
}

// tests the workflow of job cancellation without savepoint
func (s *IntegSuite) TestJobCancellationWithoutSavepoint(c *C) {

	testName := "cancelsuccess"
	const finalizer = "simple.finalizers.test.com"

	// start a simple app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = testName + "job"
	config.Spec.DeleteMode = v1beta1.DeleteModeForceCancel
	config.Spec.SavepointDisabled = true
	config.ObjectMeta.Labels["integTest"] = testName
	config.Finalizers = append(config.Finalizers, finalizer)

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(config.Name), IsNil)

	pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, config.Spec.Image)
	}

	// test updating the app with a new image
	newApp := WaitUpdateAndValidate(c, s, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.Image = NewImage
	}, v1beta1.FlinkApplicationDeployFailed)

	c.Assert(newApp.Spec.Image, Equals, NewImage)
	c.Assert(newApp.Status.SavepointPath, Equals, "")

	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NewImage)
	}

	// cleanup
	c.Assert(s.Util.FlinkApps().Delete(newApp.Name, &v1.DeleteOptions{}), IsNil)
	var app *v1beta1.FlinkApplication
	for {
		app, err = s.Util.GetFlinkApplication(config.Name)
		c.Assert(err, IsNil)
		if len(app.Finalizers) == 1 && app.Finalizers[0] == finalizer {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	job := s.Util.GetJobOverview(app)
	c.Assert(job["status"], Equals, "CANCELED")

	// delete our finalizer
	app.Finalizers = []string{}
	_, err = s.Util.FlinkApps().Update(app)
	c.Assert(err, IsNil)

	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(v1.ListOptions{LabelSelector: "integTest=" + testName})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
	}
	log.Info("All pods torn down")
}

// tests a job update with the existing job already in cancelled state.
// here, the new submitted job starts without a savepoint.
func (s *IntegSuite) TestCancelledJobWithoutSavepoint(c *C) {

	testName := "invalidcancel"
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = testName + "job"
	config.Spec.DeleteMode = v1beta1.DeleteModeForceCancel
	config.Spec.SavepointDisabled = true
	config.ObjectMeta.Labels["integTest"] = testName

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(config.Name), IsNil)

	currApp, _ := s.Util.GetFlinkApplication(config.Name)
	c.Assert(currApp.Status.SavepointPath, Equals, "")
	job := s.Util.GetJobOverview(currApp)
	c.Assert(job["status"], Equals, "RUNNING")

	// trigger a cancel on the existing job
	endpoint := fmt.Sprintf("jobs/%s?mode=cancel", currApp.Status.JobStatus.JobID)
	_, err = s.Util.FlinkAPIPatch(currApp, endpoint)
	c.Assert(err, IsNil)

	// wait a bit
	time.Sleep(1 * time.Second)

	job = s.Util.GetJobOverview(currApp)
	c.Assert(job["status"], Equals, "CANCELED")

	newApp, err := s.Util.Update(config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.Image = NewImage
	})
	c.Assert(err, IsNil)

	for {
		// wait until the new job is launched
		newApp, err := s.Util.GetFlinkApplication(config.Name)
		c.Assert(err, IsNil)
		if newApp.Status.JobStatus.JobID != "" &&
			newApp.Status.JobStatus.JobID != currApp.Status.JobStatus.JobID {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// we should end up in the Running of the new job
	c.Assert(s.Util.WaitForPhase(newApp.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)

	newApp, err = s.Util.GetFlinkApplication(newApp.Name)
	c.Assert(err, IsNil)

	job = s.Util.GetJobOverview(newApp)
	c.Assert(job["status"], Equals, "RUNNING")
	c.Assert(newApp.Status.SavepointPath, Equals, "")

	// start deleting
	c.Assert(s.Util.FlinkApps().Delete(config.Name, &v1.DeleteOptions{}), IsNil)
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(v1.ListOptions{LabelSelector: "integTest=" + testName})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
	}
	log.Info("All pods torn down")
}

// tests the recovery workflow of the job when savepoint is disabled.
func (s *IntegSuite) TestJobRecoveryWithoutSavepoint(c *C) {

	const finalizer = "simple.finalizers.test.com"
	const testName = "cancelrecovery"

	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = testName
	config.ObjectMeta.Labels["integTest"] = testName
	config.Finalizers = append(config.Finalizers, finalizer)
	config.Spec.DeleteMode = v1beta1.DeleteModeForceCancel
	config.Spec.SavepointDisabled = true

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationSavepointing), IsNil)

	c.Assert(s.Util.WaitForAllTasksRunning(config.Name), IsNil)
	currApp, _ := s.Util.GetFlinkApplication(config.Name)
	c.Assert(currApp.Status.SavepointPath, Equals, "")

	// Test updating the app with a bad jar name -- this should cause a failed deploy and roll back
	_, err = s.Util.Update(config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.JarName = "nonexistent.jar"
		app.Spec.RestartNonce = "rollback"
	})
	c.Assert(err, IsNil)
	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationDeployFailed, ""), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(config.Name), IsNil)

	// assert the restart of the job with a new job id and old deploy hash.
	newApp, err := s.Util.GetFlinkApplication(config.Name)
	c.Assert(err, IsNil)
	c.Assert(newApp.Status.JobStatus.JobID, Not(Equals), currApp.Status.JobStatus.JobID)
	c.Assert(newApp.Status.SavepointPath, Equals, "")
	c.Assert(newApp.Status.SavepointTriggerID, Equals, "")
	c.Assert(newApp.Status.DeployHash, Equals, currApp.Status.DeployHash)

	// assert that the restarted job wasn't restored from a savepoint.
	endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobStatus.JobID)
	res, err := s.Util.FlinkAPIGet(newApp, endpoint)
	c.Assert(err, IsNil)
	body := res.(map[string]interface{})
	restored := (body["latest"].(map[string]interface{}))["restored"]
	c.Assert(restored, IsNil)

	// roll forward with the right config.
	_ = WaitUpdateAndValidate(c, s, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.JarName = config.Spec.JarName
		app.Spec.RestartNonce = "rollback2"
		app.Spec.Image = NewImage
	}, "")

	// assert the pods have the new image
	pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NewImage)
	}

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(config.Name, &v1.DeleteOptions{}), IsNil)
	var app *v1beta1.FlinkApplication
	for {
		app, err = s.Util.GetFlinkApplication(config.Name)
		c.Assert(err, IsNil)
		if len(app.Finalizers) == 1 && app.Finalizers[0] == finalizer {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(app.Status.SavepointPath, Equals, "")
	c.Assert(app.Status.SavepointTriggerID, Equals, "")

	app.Finalizers = []string{}
	_, err = s.Util.FlinkApps().Update(app)
	c.Assert(err, IsNil)

	// wait until all pods are deleted
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(v1.ListOptions{LabelSelector: "integTest=" + testName})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Info("All pods torn down")
}
