package integ

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyft/flinkk8soperator/cmd/flinkk8soperator/cmd"
	integFramework "github.com/lyft/flinkk8soperator/integ/utils"
	controllerConfig "github.com/lyft/flinkk8soperator/pkg/controller/config"
	flyteConfig "github.com/lyft/flytestdlib/config"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/homedir"
)

type IntegSuite struct {
	Util *integFramework.TestUtil
}

var _ = Suite(&IntegSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

func (s *IntegSuite) SetUpSuite(c *C) {
	// var namespace = flag.String("namespace", "flinkoperatortest", "namespace to use for testing")
	var namespace = os.Getenv("NAMESPACE")
	if namespace == "" {
		namespace = "flinkoperatortest"
	}
	// var runDirect = flag.Bool("runDirect", false, "if set, runs the operator from the current source instead of from an image")
	var runDirect = os.Getenv("RUN_DIRECT") != ""
	// var image = flag.String("operatorImage", "flinkk8soperator:latest", "image for the operator")
	var image = os.Getenv("OPERATOR_IMAGE")
	if image == "" {
		image = "flinkk8soperator:latest"
	}
	//var integration = flag.Bool("integration", false, "run integration tests")
	var integration = os.Getenv("INTEGRATION") != ""

	if !integration {
		// skip integration tests unless --integration is passed
		c.Skip("--integration not provided")
		return
	}

	kubeconfig := os.Getenv("KUBERNETES_CONFIG")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
		err := os.Setenv("KUBERNETES_CONFIG", kubeconfig)
		if err != nil {
			c.Fatalf("Failed to set KUBERNETES_CONFIG env")
		}
	}

	checkpointDir := os.Getenv("CHECKPOINT_DIR")
	if checkpointDir == "" {
		checkpointDir = "/tmp/checkpoints"
	}

	var err error
	s.Util, err = integFramework.New(namespace, kubeconfig, image, checkpointDir)
	if err != nil {
		c.Fatalf("Failed to set up test util: %v", err)
	}

	if err = s.Util.CreateCRD(); err != nil && !k8sErrors.IsAlreadyExists(err) {
		c.Fatalf("Failed to create CRD: %v", err)
	}

	if runDirect {
		config := controllerConfig.Config{
			LimitNamespace: namespace,
			UseProxy:       true,
			ResyncPeriod:   flyteConfig.Duration{Duration: 3 * time.Second},
			MaxErrDuration: flyteConfig.Duration{Duration: 30 * time.Second},
			MetricsPrefix:  "flinkk8soperator",
			ProxyPort:      flyteConfig.Port{Port: 8001},
		}

		log.Info("Running operator directly")

		go func() {
			if err = cmd.Run(&config); err != nil {
				c.Fatalf("Failed to run operator: %v", err)
			}
		}()
	} else {
		if err = s.Util.CreateOperator(); err != nil {
			c.Fatalf("Failed to create operator: %v", err)
		}

		if err = s.Util.TailOperatorLogs(); err != nil {
			c.Fatalf("Failed to tail operator logs: %v", err)
		}
	}
}

func (s *IntegSuite) TearDownSuite(c *C) {
	if s != nil && s.Util != nil {
		log.Info("Cleaning up")
		s.Util.Cleanup()
	}
}

func (s *IntegSuite) SetUpTest(c *C) {
	// create checkpoint directory
	if _, err := os.Stat(s.Util.CheckpointDir); os.IsNotExist(err) {
		c.Assert(os.Mkdir(s.Util.CheckpointDir, 0777), IsNil)
	}
}

func (s *IntegSuite) TearDownTest(c *C) {
	jm, err := s.Util.GetJobManagerPod()
	if err == nil {
		fmt.Printf("\n\n######### JobManager logs for debugging #########\n---------------------------\n")
		_ = s.Util.GetLogs(jm, nil)
	}

	tms, err := s.Util.GetTaskManagerPods()
	if err == nil {
		for i, tm := range tms {
			fmt.Printf("\n\n######### TaskManager %d logs for debugging "+
				"#########\n---------------------------\n", i)
			_ = s.Util.GetLogs(tm, nil)
		}
	}

	err = s.Util.FlinkApps().DeleteCollection(nil, v1.ListOptions{})
	if err != nil {
		log.Fatalf("Failed to clean up flink applications")
	}

	err = os.RemoveAll(s.Util.CheckpointDir)
	if err != nil {
		log.Fatalf("Failed to clean up checkpoints directory: %v", err)
	}
}
