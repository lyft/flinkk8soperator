package integ

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func failingJobTest(s *IntegSuite, c *C, testName string, causeFailure func()) {
	// create a Flink app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))
	config.Name = testName + "job"
	config.Spec.DeleteMode = v1beta1.DeleteModeForceCancel

	config.ObjectMeta.Labels["integTest"] = testName

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	// Cause it to fail
	causeFailure()

	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)

	// wait a bit for it to start failing
	time.Sleep(5 * time.Second)

	// Try to update it
	app, err := s.Util.GetFlinkApplication(config.Name)
	c.Assert(err, IsNil)
	app.Spec.Image = NewImage
	_, err = s.Util.FlinkApps().Update(app)
	c.Assert(err, IsNil)

	// because the checkpoint will fail, the app should move to deploy failed
	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationDeployFailed), IsNil)

	// And the job should not have been updated
	newApp, err := s.Util.GetFlinkApplication(config.Name)
	c.Assert(err, IsNil)
	c.Assert(newApp.Status.JobStatus.JobID, Equals, app.Status.JobStatus.JobID)

	endpoint := fmt.Sprintf("jobs/%s", app.Status.JobStatus.JobID)
	_, err = s.Util.FlinkAPIGet(app, endpoint)
	c.Assert(err, IsNil)

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(app.Name, &v1.DeleteOptions{}), IsNil)

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

// Tests that we correctly handle updating a job with task failures
func (s *IntegSuite) TestJobWithTaskFailures(c *C) {
	failingJobTest(s, c, "taskfailure", func() {
		f, err := os.OpenFile(s.Util.CheckpointDir+"/fail", os.O_RDONLY|os.O_CREATE, 0666)
		c.Assert(err, IsNil)
		c.Assert(f.Close(), IsNil)
	})
}

// Tests that we correctly handle updating a job with a checkpoint timeout
func (s *IntegSuite) TestCheckpointTimeout(c *C) {
	failingJobTest(s, c, "checkpointtimeout", func() {
		// cause checkpoints to take 120 seconds
		err := ioutil.WriteFile(s.Util.CheckpointDir+"/checkpoint_delay", []byte("120000"), 0644)
		c.Assert(err, IsNil)
	})
}
