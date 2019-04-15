package integ

import (
	"fmt"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

const NEW_IMAGE = "lyft/operator-test-app:3300368e5d620e5e5e50078675f74a841f376613"

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
	c.Assert(newApp.Status.JobId, Not(Equals), app.Status.JobId)

	log.Info("New job started successfully")

	// check that we savepointed and restored correctly
	endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobId)
	res, err := s.Util.FlinkApiGet(newApp, endpoint)
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
		app.Spec.Image = NEW_IMAGE
	})
	// check that the pods have the new image
	c.Assert(newApp.Spec.Image, Equals, NEW_IMAGE)
	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=test_simple"})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NEW_IMAGE)
	}

	// test updating the app with a config change
	newApp = updateAndValidate(c, s, config.Name, func(app *v1alpha1.FlinkApplication) {
		app.Spec.FlinkConfig["akka.client.timeout"] = "23 s"
	})
	// validate the config has been applied
	res, err := s.Util.FlinkApiGet(newApp, "/jobmanager/config")
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
