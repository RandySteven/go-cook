package cronjob_client

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"
)

type (
	JobConfig struct {

	}
	Scheduler interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
	}

	scheduler struct {
		cronJob *cron.Cron
	}
)

func NewScheduler(config *JobConfig) (Scheduler, error) {
	jakartaTime, _ := time.LoadLocation("Asia/Jakarta")

	return &scheduler{
		cronJob: cron.New(cron.WithSeconds(), cron.WithLocation(jakartaTime)),
	}, nil
}
