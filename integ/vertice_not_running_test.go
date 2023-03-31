package integ

import (
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/prometheus/common/log"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *IntegSuite) TestVertexNotRunning(c *C) {
	log.Info("Starting test TestVertexNotRunning")

	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Name = "testvertexnotrunning"
	config.ObjectMeta.Labels["integTest"] = "test_vertex_not_running"
	envVar := corev1.EnvVar{
		Name:  "EXTERNAL_CHECKPOINT",
		Value: "1",
	}

	config.Spec.JobManagerConfig.EnvConfig.Env =
		append(config.Spec.JobManagerConfig.EnvConfig.Env, envVar)
	config.Spec.TaskManagerConfig.EnvConfig.Env =
		append(config.Spec.TaskManagerConfig.EnvConfig.Env, envVar)
	config.Spec.ProgramArgs = "--job_name testapp, --mode realtime"
	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	log.Info("Application Created")

	// wait for it to be running
	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationDeployFailed), IsNil)

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
	log.Info("Completed test TestRecovery")
}
