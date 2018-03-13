package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/deckarep/golang-set"
	"go.uber.org/atomic"
)

// Stats represent stats for a request
type Stats struct {
	RequestID string

	responsesFrom   mapset.Set
	discoveredNodes mapset.Set

	responses atomic.Int32
	passed    atomic.Int32
	failed    atomic.Int32

	start        time.Time
	end          time.Time
	publishStart time.Time
	publishEnd   time.Time

	DiscoveryTime time.Duration

	mu   *sync.Mutex
	once sync.Once
}

// NewStats initializes a new stats instance
func NewStats() *Stats {
	return &Stats{
		responsesFrom:   mapset.NewSet(),
		discoveredNodes: mapset.NewSet(),
		mu:              &sync.Mutex{},
	}
}

func (s *Stats) showProgress(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			discovered := s.DiscoveredCount()

			fmt.Printf("ok: %-5d failed: %-5d received: %d / %d\n", s.passed.Load(), s.failed.Load(), s.ResponsesCount(), discovered)
		case <-ctx.Done():
			return
		}
	}
}

// All determines if all expected nodes replied already
func (s *Stats) All() bool {
	if int(s.responses.Load()) < s.discoveredNodes.Cardinality() {
		return false
	}

	return len(s.NoResponseFrom()) == 0
}

// StartProgress starts a basic progress display that will be interrupted by the context
func (s *Stats) StartProgress(ctx context.Context) {
	s.once.Do(func() { go s.showProgress(ctx) })
}

// NoResponseFrom calculates discovered which hosts did not respond
func (s *Stats) NoResponseFrom() []string {
	diff := s.discoveredNodes.Difference(s.responsesFrom)

	r := []string{}
	for n := range diff.Iter() {
		r = append(r, n.(string))
	}

	return r
}

// UnexpectedResponseFrom calculates which hosts responses that we did not expect responses from
func (s *Stats) UnexpectedResponseFrom() []string {
	diff := s.responsesFrom.Difference(s.discoveredNodes)

	r := []string{}
	for n := range diff.Iter() {
		r = append(r, n.(string))
	}

	return r
}

// SetDiscoveredNodes records the node names we expect to communicate with
func (s *Stats) SetDiscoveredNodes(nodes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.discoveredNodes.Clear()

	for _, node := range nodes {
		s.discoveredNodes.Add(node)
	}
}

// FailedRequestInc increments the failed request counter by one
func (s *Stats) FailedRequestInc() {
	s.failed.Inc()
}

// PassedRequestInc increments the passed request counter by one
func (s *Stats) PassedRequestInc() {
	s.passed.Inc()
}

// RecordReceived reords the fact that one message was received
func (s *Stats) RecordReceived(sender string) {
	s.responses.Inc()

	s.responsesFrom.Add(sender)
}

// DiscoveredCount is how many nodes were discovered
func (s *Stats) DiscoveredCount() int {
	return s.discoveredNodes.Cardinality()
}

// FailCount is the number of responses that were failures
func (s *Stats) FailCount() int {
	return int(s.failed.Load())
}

// OKCount is the number of responses that were ok
func (s *Stats) OKCount() int {
	return int(s.passed.Load())
}

// ResponsesCount if the total amount of nodes that responded so far
func (s *Stats) ResponsesCount() int {
	return int(s.responses.Load())
}

// StartPublish records the publish started
func (s *Stats) StartPublish() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.publishStart.IsZero() {
		s.publishStart = time.Now()
	}
}

// EndPublish records the publish process ended
func (s *Stats) EndPublish() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.publishEnd.IsZero() {
		s.publishEnd = time.Now()
	}
}

// PublishDuration calculates how long publishing took
func (s *Stats) PublishDuration() (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.publishStart.IsZero() || s.publishEnd.IsZero() {
		return time.Duration(0), fmt.Errorf("publishing is not completed")
	}

	return s.publishEnd.Sub(s.publishStart), nil
}

// RequestDuration calculates the total duration
func (s *Stats) RequestDuration() (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.start.IsZero() || s.end.IsZero() {
		return time.Duration(0), fmt.Errorf("request is not completed")
	}

	return s.end.Sub(s.start), nil
}

// Start records the start time of a request
func (s *Stats) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.start.IsZero() {
		s.start = time.Now()
	}
}

// End records the end time of a request
func (s *Stats) End() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.end.IsZero() {
		s.end = time.Now()
	}
}
