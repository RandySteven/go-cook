package nexus

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

type (
	Deunamist struct {
		Namespace string
		Service   interface{}
	}

	Nexus interface {
		RegisterWorkflow(ctx context.Context, deunamist *Deunamist)
		ExecuteNexus(ctx context.Context, endpoint string, requestData interface{}, resultData interface{}) (err error)
	}

	nexusClient struct {
		nexusClient workflow.NexusClient
	}
)

func NewNexus() {

}