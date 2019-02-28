package flink

import (
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/stretchr/testify/assert"
	"sort"
	"strings"
	"testing"
)

func TestRenderFlinkConfigOverrides(t *testing.T) {
	yaml, err := renderFlinkConfigOverrides(v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationNew,
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			FlinkConfig: map[string]interface{}{
				"akka.timeout":                            "5s",
				"taskmanager.network.memory.fraction":     0.1,
				"taskmanager.network.request-backoff.max": 5000,
			},
		},
	})

	if err != nil {
		assert.NoError(t, err, "Got error producing config")
	}

	lines := strings.Split(strings.TrimSpace(yaml), "\n")
	sort.Strings(lines)

	expected := []string{
		"akka.timeout: 5s",
		"taskmanager.network.memory.fraction: 0.1",
		"taskmanager.network.request-backoff.max: 5000",
	}

	assert.Equal(t, expected, lines)
}
