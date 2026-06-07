package cronjob_client

import "context"

func (s *scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cronJob.Start()
	return nil
}

func (s *scheduler) Stop(ctx context.Context) error {
	cronCtx := s.cronJob.Stop()

	select {
	case <-cronCtx.Done():
		return cronCtx.Err()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
