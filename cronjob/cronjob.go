package cronjob_client

import (
	"context"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type (
	JobConfig struct {
		LoadLocation string
	}

	Scheduler interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
	}

	scheduler struct {
		cronJob *cron.Cron
		mu      sync.Mutex
	}
)

func NewScheduler(config *JobConfig) (Scheduler, error) {
	schedulerTime, err := time.LoadLocation(config.LoadLocation)
	if err != nil {
		return nil, err
	}

	return &scheduler{
		cronJob: cron.New(cron.WithSeconds(), cron.WithLocation(schedulerTime)),
	}, nil
}
