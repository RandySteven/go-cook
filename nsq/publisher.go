package nsq_client

import "context"

// Publish sends a message body to the specified NSQ topic.
// Returns an error if publishing fails.
func (n *nsqClient) Publish(ctx context.Context, topic string, body []byte) error {
	return n.pub.Publish(topic, body)
}
