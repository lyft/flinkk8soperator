package integ

import (
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func WaitForUpdate(c *C, s *IntegSuite, name string, updateFn func(app *v1beta1.FlinkApplication), phase v1beta1.FlinkApplicationPhase, failurePhase v1beta1.FlinkApplicationPhase) *v1beta1.FlinkApplication {

	// update with new image.
	app, err := s.Util.Update(name, updateFn)
	c.Assert(err, IsNil)

	for {
		// keep trying until the new job is launched
		newApp, err := s.Util.GetFlinkApplication(name)
		c.Assert(err, IsNil)
		if newApp.Status.VersionStatuses[s.Util.GetCurrentStatusIndex(app)].JobStatus.JobID != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(s.Util.WaitForPhase(name, phase, failurePhase), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(name), IsNil)

	newApp, _ := s.Util.GetFlinkApplication(name)
	return newApp
}

func (s *IntegSuite) TestUpdateWithBlueGreenDeploymentMode(c *C) {

	testName := "bluegreenupdate"
	const finalizer = "bluegreen.finalizers.test.com"

	// start a simple app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = testName + "job"
	config.Spec.DeploymentMode = v1beta1.DeploymentModeBlueGreen
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
	newApp := WaitForUpdate(c, s, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.Image = NewImage
	}, v1beta1.FlinkApplicationDualRunning, v1beta1.FlinkApplicationDeployFailed)

	c.Assert(newApp.Spec.Image, Equals, NewImage)
	c.Assert(newApp.Status.SavepointPath, NotNil)

	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	// We have 2 applications running
	c.Assert(len(pods.Items), Equals, 6)
	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationDualRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.GetJobID(newApp), NotNil)
	c.Assert(newApp.Status.UpdatingVersion, Equals, v1beta1.BlueFlinkApplication)
	c.Assert(newApp.Status.DeployVersion, Equals, v1beta1.GreenFlinkApplication)

	// TearDownVersionHash
	teardownVersion := newApp.Status.DeployVersion
	hashToTeardown := newApp.Status.DeployHash
	oldHash := newApp.Status.DeployHash
	log.Infof("Tearing down version %s", teardownVersion)
	newApp = WaitForUpdate(c, s, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.TearDownVersionHash = hashToTeardown
	}, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed)

	// wait for the old cluster to be cleaned up
	for {
		pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).List(v1.ListOptions{})
		c.Assert(err, IsNil)

		oldPodFound := false

		for _, pod := range pods.Items {
			if pod.Annotations["flink-app-hash"] == oldHash {
				oldPodFound = true
			}
		}

		if !oldPodFound {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(newApp.Status.TeardownHash, NotNil)
	c.Assert(newApp.Status.DeployVersion, Equals, v1beta1.BlueFlinkApplication)
	c.Assert(newApp.Status.VersionStatuses[0].JobStatus.JobID, NotNil)
	c.Assert(newApp.Status.VersionStatuses[1].JobStatus, Equals, v1beta1.FlinkJobStatus{})

	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "flink-app-hash=" + oldHash})
	for _, pod := range pods.Items {
		log.Infof("Pod name %s", pod.Name)
		c.Assert(pod.Labels["flink-application-version"], Not(Equals), teardownVersion)
	}

	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 0)

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
	c.Assert(app.Status.SavepointPath, NotNil)

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
