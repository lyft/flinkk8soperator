package flink

import (
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestReplaceJobUrl(t *testing.T) {
	assert.Equal(t,
		"ABC.ingress.flyte.us-east-1.k8s.lyft.net",
		ReplaceJobUrl("{{$jobCluster}}.ingress.flyte.us-east-1.k8s.lyft.net", "ABC"))
}

func TestGetFlinkUIIngressURL(t *testing.T) {
	config.Init("")
	assert.Equal(t,
		"ABC.ingress.flyte.us-east-1.k8s.lyft.net",
		GetFlinkUIIngressURL("ABC"))
}
