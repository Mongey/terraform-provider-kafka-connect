package connect

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// withRebalanceRetry executes the provided function with exponential backoff
// retry logic specifically designed to handle Kafka Connect rebalancing
// scenarios. The timeout parameter specifies how long to wait for rebalancing
// to complete.
func withRebalanceRetry(fn func() error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := 250 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if !isRebalanceError(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Kafka Connect rebalance to finish: %w", err)
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		sleep := backoff + jitter
		log.Printf("[INFO] Connect rebalance in progress; retrying after %.2fs ... (%v)", sleep.Seconds(), err)
		time.Sleep(sleep)
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// isRebalanceError tries to detect the Connect 409 window and related exceptions.
func isRebalanceError(err error) bool {
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "rebalance") ||
		strings.Contains(msg, "rebalanceexpected") ||
		strings.Contains(msg, "rebalance is expected") ||
		strings.Contains(msg, "conflicting operation") {
		return true
	}

	return strings.Contains(msg, "409")
}

func nameFromRD(d *schema.ResourceData) string {
	return d.Get("name").(string)
}
