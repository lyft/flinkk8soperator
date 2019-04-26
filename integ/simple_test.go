package integ

import (
	"encoding/json"
	"fmt"

	"os"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const NewImage = "lyft/operator-test-app:c371d09946d7b328e5a8f5fdc5089f5247f60088"

func updateAndValidate(c *C, s *IntegSuite, name string, updateFn func(app *v1alpha1.FlinkApplication)) *v1alpha1.FlinkApplication {
	app, err := s.Util.GetFlinkApplication(name)
	c.Assert(err, IsNil)

	// Update the app
	updateFn(app)

	_, err = s.Util.FlinkApps().Update(app)
	c.Assert(err, IsNil)

	c.Assert(s.Util.WaitForPhase(name, v1alpha1.FlinkApplicationSavepointing, v1alpha1.FlinkApplicationFailed), IsNil)
	c.Assert(s.Util.WaitForPhase(name, v1alpha1.FlinkApplicationRunning, v1alpha1.FlinkApplicationFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksInState(name, "RUNNING"), IsNil)

	// check that it really updated
	newApp, err := s.Util.GetFlinkApplication(name)
	c.Assert(err, IsNil)
	c.Assert(newApp.Status.JobStatus.JobID, Not(Equals), app.Status.JobStatus.JobID)

	log.Info("New job started successfully")

	// check that we savepointed and restored correctly
	endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobStatus.JobID)
	res, err := s.Util.FlinkAPIGet(newApp, endpoint)
	c.Assert(err, IsNil)

	body := res.(map[string]interface{})
	restored := (body["latest"].(map[string]interface{}))["restored"]
	c.Assert(restored, NotNil)

	c.Assert(restored.(map[string]interface{})["is_savepoint"], Equals, true)

	return newApp
}

// Tests job submission, upgrade, and deletion
func (s *IntegSuite) TestSimple(c *C) {
	// start a simple app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.ObjectMeta.Labels["integTest"] = "test_simple"

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	c.Assert(s.Util.WaitForPhase(config.Name, v1alpha1.FlinkApplicationRunning, v1alpha1.FlinkApplicationFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksInState(config.Name, "RUNNING"), IsNil)

	pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=test_simple"})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, config.Spec.Image)
	}

	log.Info("Application started successfully")

	// test updating the app with a new image
	newApp := updateAndValidate(c, s, config.Name, func(app *v1alpha1.FlinkApplication) {
		app.Spec.Image = NewImage
	})
	// check that the pods have the new image
	c.Assert(newApp.Spec.Image, Equals, NewImage)
	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=test_simple"})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NewImage)
	}

	// test updating the app with a config change
	newApp = updateAndValidate(c, s, config.Name, func(app *v1alpha1.FlinkApplication) {
		app.Spec.FlinkConfig["akka.client.timeout"] = "23 s"
	})
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

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(config.Name, &v1.DeleteOptions{}), IsNil)

	// TODO: validate that the job is cancelled with savepoint once that's implemented [STRMCMP-206]

	// wait until all pods are gone
	for {
		pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(v1.ListOptions{LabelSelector: "integTest=test_simple"})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
	}
	log.Info("All pods torn down")
}

func (s *IntegSuite) TestRecovery(c *C) {
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = "testrecoveryjob"
	config.ObjectMeta.Labels["integTest"] = "test_recovery"
	envVar := corev1.EnvVar{
		Name:  "EXTERNAL_CHECKPOINT",
		Value: "1",
	}

	config.Spec.JobManagerConfig.Environment.Env =
		append(config.Spec.JobManagerConfig.Environment.Env, envVar)
	config.Spec.TaskManagerConfig.Environment.Env =
		append(config.Spec.TaskManagerConfig.Environment.Env, envVar)

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	// wait for it to be running
	c.Assert(s.Util.WaitForPhase(config.Name, v1alpha1.FlinkApplicationRunning, v1alpha1.FlinkApplicationFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksInState(config.Name, "RUNNING"), IsNil)

	c.Log("Application running")

	// wait for checkpoints
	app, err := s.Util.GetFlinkApplication(config.Name)
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
	f, err := os.OpenFile(s.Util.CheckpointDir+"/fail", os.O_RDONLY|os.O_CREATE, 0666)
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)

	// wait a bit
	time.Sleep(1 * time.Second)

	// try to update the job
	app.Spec.Image = NewImage
	_, err = s.Util.FlinkApps().Update(app)

	for {
		// wait until the new job is launched
		newApp, err := s.Util.GetFlinkApplication(config.Name)
		c.Assert(err, IsNil)
		if newApp.Status.JobStatus.JobID != app.Status.JobStatus.JobID {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(err, IsNil)
	c.Assert(s.Util.WaitForPhase(config.Name, v1alpha1.FlinkApplicationRunning, v1alpha1.FlinkApplicationFailed), IsNil)

	// stop it from failing
	c.Assert(os.Remove(s.Util.CheckpointDir+"/fail"), IsNil)
	c.Assert(s.Util.WaitForAllTasksInState(config.Name, "RUNNING"), IsNil)

	// delete the application
	c.Assert(s.Util.FlinkApps().Delete(config.Name, &v1.DeleteOptions{}), IsNil)
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(v1.ListOptions{LabelSelector: "integTest=test_recovery"})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
	}
	log.Info("All pods torn down")
}
