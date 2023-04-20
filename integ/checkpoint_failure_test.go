package integ

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	. "gopkg.in/check.v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"time"
)

func failingJobTest(ctx context.Context, s *IntegSuite, c *C, testName string, causeFailure func(), logger log.Logger) {
	// create a Flink app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))
	config.Name = testName + "job"
	config.Spec.DeleteMode = v1beta1.DeleteModeForceCancel

	config.ObjectMeta.Labels["integTest"] = testName

	c.Assert(s.Util.CreateFlinkApplication(ctx, config), IsNil,
		Commentf("Failed to create flink application"))

	// Cause it to fail
	causeFailure()

	c.Assert(s.Util.WaitForPhase(ctx, config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)

	// wait a bit for it to start failing
	time.Sleep(5 * time.Second)

	// Try to update it
	app, err := s.Util.GetFlinkApplication(ctx, config.Name)
	c.Assert(err, IsNil)
	app.Spec.Image = NewImage
	_, err = s.Util.FlinkApps().Update(ctx, app)
	c.Assert(err, IsNil)

	// because the checkpoint will fail, the app should move to deploy failed
	c.Assert(s.Util.WaitForPhase(ctx, config.Name, v1beta1.FlinkApplicationDeployFailed), IsNil)

	// And the job should not have been updated
	newApp, err := s.Util.GetFlinkApplication(ctx, config.Name)
	c.Assert(err, IsNil)
	c.Assert(newApp.Status.JobStatus.JobID, Equals, app.Status.JobStatus.JobID)

	endpoint := fmt.Sprintf("jobs/%s", app.Status.JobStatus.JobID)
	_, err = s.Util.FlinkAPIGet(app, endpoint)
	c.Assert(err, IsNil)

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(ctx, app.Name, &v1.DeleteOptions{}), IsNil)

	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(ctx, v1.ListOptions{LabelSelector: "integTest=" + testName})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
	}
	logErr := logger.Log("message", "All pods torn down")
	if logErr != nil {
		return
	}
}

// Tests that we correctly handle updating a job with task failures
func (s *IntegSuite) TestJobWithTaskFailures(c *C) {
	logger := log.NewLogfmtLogger(os.Stdout)
	err := logger.Log("message", "Starting test TestJobWithTaskFailures")
	if err != nil {
		return
	}
	ctx := context.Background()
	failingJobTest(ctx, s, c, "taskfailure", func() {
		err := s.Util.ExecuteCommand("minikube", "ssh", "touch /tmp/checkpoints/fail && chmod 0644 /tmp/checkpoints/fail")
		c.Assert(err, IsNil)
	}, logger)
	err = logger.Log("message", "Completed test TestJobWithTaskFailures")
	if err != nil {
		return
	}
}

// Tests that we correctly handle updating a job with a checkpoint timeout
func (s *IntegSuite) TestCheckpointTimeout(c *C) {
	logger := log.NewLogfmtLogger(os.Stdout)
	err := logger.Log("message", "Starting test TestCheckpointTimeout")
	if err != nil {
		return
	}
	ctx := context.Background()
	failingJobTest(ctx, s, c, "checkpointtimeout", func() {
		// cause checkpoints to take 120 seconds
		err := s.Util.ExecuteCommand("minikube", "ssh", "echo 120000 >> /tmp/checkpoints/checkpoint_delay && sudo chmod 0644 /tmp/checkpoints/checkpoint_delay")
		c.Assert(err, IsNil)
	}, logger)
	err = logger.Log("message", "Completed test TestCheckpointTimeout")
	if err != nil {
		return
	}
}
