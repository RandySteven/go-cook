package nsq_client

import (
	"context"
	"fmt"
	"log"
)

// Consume retrieves the message body from the context for the specified topic.
// This is typically used within a handler function registered via RegisterConsumer.
// Returns an error if no message is found in the context for the topic.
func (n *nsqClient) Consume(ctx context.Context, topic string) (string, error) {
	if ctx.Value(topic) != nil {
		log.Println(`context value : `, ctx.Value(topic).(string))
		return ctx.Value(topic).(string), nil
	} else {
		log.Println(`context value : `, nil)
		return "", fmt.Errorf(`failed to consume the topic %s`, topic)
	}
}
