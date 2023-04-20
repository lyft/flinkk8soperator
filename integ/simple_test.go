package integ

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-kit/log"
	"os"

	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	. "gopkg.in/check.v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const NewImage = "lyft/operator-test-app:b1b3cb8e8f98bd41f44f9c89f8462ce255e0d13f.2"

func updateAndValidate(c *C, s *IntegSuite, ctx context.Context, name string, updateFn func(app *v1beta1.FlinkApplication), failurePhase v1beta1.FlinkApplicationPhase, logger log.Logger) *v1beta1.FlinkApplication {
	app, err := s.Util.Update(ctx, name, updateFn)
	c.Assert(err, IsNil)

	c.Assert(s.Util.WaitForPhase(ctx, name, v1beta1.FlinkApplicationSavepointing, failurePhase), IsNil)
	c.Assert(s.Util.WaitForPhase(ctx, name, v1beta1.FlinkApplicationRunning, failurePhase), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(ctx, name), IsNil)

	// check that it really updated
	newApp, err := s.Util.GetFlinkApplication(ctx, name)
	c.Assert(err, IsNil)
	c.Assert(newApp.Status.JobStatus.JobID, Not(Equals), app.Status.JobStatus.JobID)

	logger.Log("message", "New job started successfully")

	// check that we savepointed and restored correctly
	endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobStatus.JobID)
	res, err := s.Util.FlinkAPIGet(newApp, endpoint)
	c.Assert(err, IsNil)

	body := res.(map[string]interface{})
	restored := (body["latest"].(map[string]interface{}))["restored"]
	c.Assert(restored, NotNil)

	c.Assert(restored.(map[string]interface{})["is_savepoint"], Equals, true)

	// wait for the old cluster to be cleaned up
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(ctx, v1.ListOptions{LabelSelector: "flink-app=" + name})
		c.Assert(err, IsNil)

		oldPodFound := false

		for _, pod := range pods.Items {
			if pod.Annotations["flink-app-hash"] == app.Status.DeployHash ||
				pod.Annotations["flink-app-hash"] == app.Status.InPlaceUpdatedFromHash {
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

// Tests job submission, upgrade, rollback, and deletion
func (s *IntegSuite) TestSimple(c *C) {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger.Log("message", "Starting test TestSimple")
	ctx := context.Background()
	const finalizer = "simple.finalizers.test.com"

	// start a simple app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.ObjectMeta.Labels["integTest"] = "test_simple"
	// add a finalizer so that the flinkapplication won't be deleted until we've had a chance to look at it
	config.Finalizers = append(config.Finalizers, finalizer)

	c.Assert(s.Util.CreateFlinkApplication(ctx, config), IsNil,
		Commentf("Failed to create flink application"))

	c.Assert(s.Util.WaitForPhase(ctx, config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(ctx, config.Name), IsNil)

	pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(ctx, v1.ListOptions{LabelSelector: "integTest=test_simple"})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 2)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, config.Spec.Image)
	}

	logger.Log("message", "Application started successfully")

	// test updating the app with a new image
	newApp := updateAndValidate(c, s, ctx, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.Image = NewImage
	}, v1beta1.FlinkApplicationDeployFailed, logger)
	// check that the pods have the new image
	c.Assert(newApp.Spec.Image, Equals, NewImage)
	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(ctx, v1.ListOptions{LabelSelector: "integTest=test_simple"})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 2)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NewImage)
	}

	// test updating the app with a config change
	newApp = updateAndValidate(c, s, ctx, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.FlinkConfig["akka.client.timeout"] = "23 s"
	}, v1beta1.FlinkApplicationDeployFailed, logger)
	// validate the config has been applied
	res, err := s.Util.FlinkAPIGet(newApp, "/jobmanager/config")
	c.Assert(err, IsNil)
	body := res.([]interface{})
	value := func() interface{} {
		for _, e := range body {
			kv := e.(map[string]interface{})
			if kv["key"] == "akka.client.timeout" {
				return kv["value"]
			}
		}
		return nil
	}()
	c.Assert(value, Equals, "23 s")

	// Test updating the app with a bad jar name -- this should cause a failed deploy and roll back

	{
		logger.Log("message", "Testing rollback")
		newApp, err := s.Util.Update(ctx, config.Name, func(app *v1beta1.FlinkApplication) {
			app.Spec.JarName = "nonexistent.jar"
			// this shouldn't be needed after STRMCMP-473 is fixed
			app.Spec.RestartNonce = "rollback"
		})

		c.Assert(err, IsNil)

		c.Assert(s.Util.WaitForPhase(ctx, newApp.Name, v1beta1.FlinkApplicationSavepointing, ""), IsNil)
		// we should end up in the DeployFailed phase
		c.Assert(s.Util.WaitForPhase(ctx, newApp.Name, v1beta1.FlinkApplicationDeployFailed, ""), IsNil)

		logger.Log("message", "Job is in deploy failed, waiting for tasks to start")

		// but the job should have been resubmitted
		c.Assert(s.Util.WaitForAllTasksRunning(ctx, newApp.Name), IsNil)

		// the job id should have changed
		jobID := newApp.Status.JobStatus.JobID
		newApp, err = s.Util.GetFlinkApplication(ctx, newApp.Name)
		c.Assert(err, IsNil)
		c.Assert(newApp.Status.JobStatus.JobID, Not(Equals), jobID)

		// we should have restored from our savepoint
		endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobStatus.JobID)
		res, err := s.Util.FlinkAPIGet(newApp, endpoint)
		c.Assert(err, IsNil)

		body := res.(map[string]interface{})
		restored := (body["latest"].(map[string]interface{}))["restored"]
		c.Assert(restored, NotNil)

		c.Assert(restored.(map[string]interface{})["is_savepoint"], Equals, true)

		logger.Log("message", "Attempting to roll forward")

		// and we should be able to roll forward by resubmitting with a fixed config
		updateAndValidate(c, s, ctx, config.Name, func(app *v1beta1.FlinkApplication) {
			app.Spec.JarName = config.Spec.JarName
			app.Spec.RestartNonce = "rollback2"
		}, "", logger)
	}

	// Test force rollback of an active deploy

	{
		logger.Log("message", "Testing force rollback")
		newApp, err := s.Util.Update(ctx, config.Name, func(app *v1beta1.FlinkApplication) {
			app.Spec.Image = "lyft/badimage:latest"
		})

		c.Assert(err, IsNil)
		c.Assert(s.Util.WaitForPhase(ctx, newApp.Name, v1beta1.FlinkApplicationClusterStarting, ""), IsNil)

		// User realizes error and  cancels the deploy
		logger.Log("message", "Cancelling deploy...")
		newApp, err = s.Util.GetFlinkApplication(ctx, config.Name)
		c.Assert(err, IsNil)

		newApp.Spec.ForceRollback = true
		newApp, err = s.Util.FlinkApps().Update(ctx, newApp)
		c.Assert(err, IsNil)

		// we should end up in the DeployFailed phase
		c.Assert(s.Util.WaitForPhase(ctx, newApp.Name, v1beta1.FlinkApplicationDeployFailed, ""), IsNil)
		c.Assert(newApp.Spec.ForceRollback, Equals, true)
		logger.Log("message", "User cancelled deploy. Job is in deploy failed, waiting for tasks to start")

		// but the job should still be running
		c.Assert(newApp.Status.JobStatus.State, Equals, v1beta1.Running)
		logger.Log("message", "Attempting to roll forward with fix")

		// Fixing update
		// and we should be able to roll forward by resubmitting with a fixed config
		updateAndValidate(c, s, ctx, config.Name, func(app *v1beta1.FlinkApplication) {
			app.Spec.Image = NewImage
			app.Spec.RestartNonce = "rollback3"
			app.Spec.ForceRollback = false
		}, "", logger)
	}

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(ctx, config.Name, &v1.DeleteOptions{}), IsNil)

	// validate that a savepoint was taken and the job was cancelled
	var app *v1beta1.FlinkApplication
	for {
		app, err = s.Util.GetFlinkApplication(ctx, config.Name)
		c.Assert(err, IsNil)

		if len(app.Finalizers) == 1 && app.Finalizers[0] == finalizer {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(app.Status.SavepointPath, NotNil)
	job := func() map[string]interface{} {
		jobs, _ := s.Util.FlinkAPIGet(app, "/jobs")
		jobMap := jobs.(map[string]interface{})
		jobList := jobMap["jobs"].([]interface{})
		for _, j := range jobList {
			job := j.(map[string]interface{})
			if job["id"] == app.Status.JobStatus.JobID {
				return job
			}
		}
		return nil
	}()

	fmt.Printf("test job = %v", job)
	c.Assert(job["status"], Equals, "CANCELED")

	// delete our finalizer
	app.Finalizers = []string{}
	_, err = s.Util.FlinkApps().Update(ctx, app)
	c.Assert(err, IsNil)

	// wait until all pods are gone
	for {
		pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(ctx, v1.ListOptions{LabelSelector: "integTest=test_simple"})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	logger.Log("message", "All pods torn down")
	logger.Log("message", "Completed test TestSimple")
}

func (s *IntegSuite) TestRecovery(c *C) {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger.Log("message", "Starting test TestRecovery")
	ctx := context.Background()
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = "testrecoveryjob"
	config.ObjectMeta.Labels["integTest"] = "test_recovery"
	envVar := corev1.EnvVar{
		Name:  "EXTERNAL_CHECKPOINT",
		Value: "1",
	}

	config.Spec.JobManagerConfig.EnvConfig.Env =
		append(config.Spec.JobManagerConfig.EnvConfig.Env, envVar)
	config.Spec.TaskManagerConfig.EnvConfig.Env =
		append(config.Spec.TaskManagerConfig.EnvConfig.Env, envVar)

	c.Assert(s.Util.CreateFlinkApplication(ctx, config), IsNil,
		Commentf("Failed to create flink application"))

	logger.Log("message", "Application Created")

	// wait for it to be running
	c.Assert(s.Util.WaitForPhase(ctx, config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(ctx, config.Name), IsNil)

	logger.Log("message", "Application running")

	// wait for checkpoints
	app, err := s.Util.GetFlinkApplication(ctx, config.Name)
	c.Assert(err, IsNil)

	endpoint := fmt.Sprintf("jobs/%s/checkpoints", app.Status.JobStatus.JobID)
	for {
		res, err := s.Util.FlinkAPIGet(app, endpoint)
		c.Assert(err, IsNil)

		body, err := json.Marshal(res)
		c.Assert(err, IsNil)

		var checkpoints client.CheckpointResponse
		err = json.Unmarshal(body, &checkpoints)
		c.Assert(err, IsNil)

		if checkpoints.Latest.Completed != nil {
			c.Logf("Checkpoint created %s", checkpoints.Latest.Completed.ExternalPath)
			break
		}
	}

	// cause the app to start failing
	err = s.Util.ExecuteCommand("minikube", "ssh", "touch /tmp/checkpoints/fail && chmod 0644 /tmp/checkpoints/fail")
	c.Assert(err, IsNil)

	logger.Log("message", "Triggered failure")

	// wait a bit
	time.Sleep(1 * time.Second)

	// try to update the job
	app, err = s.Util.Update(ctx, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.Image = NewImage
	})
	c.Assert(err, IsNil)

	logger.Log("message", "Updated app")

	for {
		// wait until the new job is launched
		newApp, err := s.Util.GetFlinkApplication(ctx, config.Name)
		c.Assert(err, IsNil)
		if newApp.Status.JobStatus.JobID != app.Status.JobStatus.JobID {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(err, IsNil)
	c.Assert(s.Util.WaitForPhase(ctx, config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)

	// stop it from failing
	c.Assert(s.Util.ExecuteCommand("minikube", "ssh", "sudo rm /tmp/checkpoints/fail"), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(ctx, config.Name), IsNil)

	// delete the application
	c.Assert(s.Util.FlinkApps().Delete(ctx, config.Name, &v1.DeleteOptions{}), IsNil)
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(ctx, v1.ListOptions{LabelSelector: "integTest=test_recovery"})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
	}
	logger.Log("message", "All pods torn down")
	logger.Log("message", "Completed test TestRecovery")
}
