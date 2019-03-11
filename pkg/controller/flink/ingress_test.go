package flink

import (
	"testing"

	config2 "github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/stretchr/testify/assert"
)

func TestReplaceJobUrl(t *testing.T) {
	assert.Equal(t,
		"ABC.lyft.xyz",
		ReplaceJobUrl("{{$jobCluster}}.lyft.xyz", "ABC"))
}

func initTestConfig() {
	config2.ConfigSection.SetConfig(&config2.Config{
		FlinkIngressUrlFormat: "{{$jobCluster}}.lyft.xyz",
	})
}
func TestGetFlinkUIIngressURL(t *testing.T) {
	initTestConfig()
	assert.Equal(t,
		"ABC.lyft.xyz",
		GetFlinkUIIngressURL("ABC"))
}
