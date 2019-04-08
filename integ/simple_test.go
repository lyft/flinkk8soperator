package integ

import (
	"fmt"
	integFramework "github.com/lyft/flinkk8soperator/integ/utils"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

const NEW_IMAGE = "lyft/operator-test-app:cccd70864965fb2c24bf05bfcb0c95711e505270"

// Tests job submission, upgrade, and deletion
func (s *IntegSuite) TestSimple(c *C) {
	// start a simple app
	config, err := integFramework.ReadFlinkApplication("test_app.yaml")
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

	app, err := s.Util.GetFlinkApplication(config.Name)
	c.Assert(err, IsNil)

	app.Spec.Image = NEW_IMAGE

	_, err = s.Util.FlinkApps().Update(app)
	c.Assert(err, IsNil)

	c.Assert(s.Util.WaitForPhase(config.Name, v1alpha1.FlinkApplicationSavepointing, v1alpha1.FlinkApplicationFailed), IsNil)
	c.Assert(s.Util.WaitForPhase(config.Name, v1alpha1.FlinkApplicationRunning, v1alpha1.FlinkApplicationFailed), IsNil)

	// check that it really updated
	newApp, err := s.Util.GetFlinkApplication(config.Name)
	c.Assert(err, IsNil)
	c.Assert(newApp.Spec.Image, Equals, NEW_IMAGE)
	c.Assert(newApp.Status.JobId, Not(Equals), app.Status.JobId)

	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=test_simple"})
	c.Assert(len(pods.Items), Equals, 3)
	c.Assert(err, IsNil)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NEW_IMAGE)
	}

	log.Info("New job started successfully")

	// check that we savepointed and restored correctly
	endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobId)
	body, err := s.Util.FlinkApiGet(newApp, endpoint)
	c.Assert(err, IsNil)

	restored := (body["latest"].(map[string]interface{}))["restored"]
	c.Assert(restored, NotNil)

	c.Assert(restored.(map[string]interface{})["is_savepoint"], Equals, true)

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(app.Name, &v1.DeleteOptions{}), IsNil)

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
