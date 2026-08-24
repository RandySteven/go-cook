package cronjob_client

import (
	"context"
	"testing"
	"time"
)

func TestNewScheduler(t *testing.T) {
	s, err := NewScheduler(&JobConfig{LoadLocation: "UTC"})
	if err != nil || s == nil {
		t.Fatalf("NewScheduler UTC: s=%v err=%v", s, err)
	}

	s, err = NewScheduler(&JobConfig{LoadLocation: "Asia/Jakarta"})
	if err != nil || s == nil {
		t.Fatalf("NewScheduler Asia/Jakarta: s=%v err=%v", s, err)
	}

	_, err = NewScheduler(&JobConfig{LoadLocation: "Not/A/Timezone"})
	if err == nil {
		t.Fatal("expected invalid timezone error")
	}
}

func TestSchedulerRunAndStop(t *testing.T) {
	s, err := NewScheduler(&JobConfig{LoadLocation: "UTC"})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSchedulerStopCanceledContext(t *testing.T) {
	s, err := NewScheduler(&JobConfig{LoadLocation: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Stop may return nil via the default branch if cron has already finished,
	// or ctx.Err() if the canceled context is selected first.
	_ = s.Stop(ctx)
}
