package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/choria-io/go-choria/choria"
	"github.com/choria-io/go-protocol/protocol"
	log "github.com/sirupsen/logrus"
)

// RequestOptions are options for a RPC request
type RequestOptions struct {
	Targets         []string
	Batched         bool
	BatchSize       int
	BatchSleep      time.Duration
	ProtocolVersion string
	RequestType     string
	RequestID       string
	Workers         int
	Collective      string
	ReplyTo         string
	ProcessReplies  bool
	ReceiveReplies  bool
	Replies         chan *choria.ConnectorMessage
	ReceiverReady   chan interface{}
	Publisher       ClientConnector
	stats           *Stats
	Progress        bool
	TimeOut         time.Duration
	Handler         func(protocol.Reply, *RPCReply)

	mu     *sync.Mutex
	wg     *sync.WaitGroup
	ctx    context.Context
	cancel func()
}

// RequestOption is a function capable of setting an option
type RequestOption func(*RequestOptions)

// Close closes all the connections associated with a request
//
// The channel used to receive messages will also be closed
func (o *RequestOptions) Close() {
	log.Debugf("Closing request %s", o.RequestID)

	o.cancel()

	o.wg.Wait()

	if o.Replies != nil {
		close(o.Replies)
	}
}

// Stats retrieves the stats for the completed request
func (o *RequestOptions) Stats() *Stats {
	return o.stats
}

// WithProgress enable a progress writer
func WithProgress() RequestOption {
	return func(o *RequestOptions) {
		o.Progress = true
	}
}

// WithTargets configures targets for a RPC request
func WithTargets(t []string) RequestOption {
	return func(o *RequestOptions) {
		o.Targets = t
	}
}

// WithProtocol sets the protocol version to use
func WithProtocol(v string) RequestOption {
	return func(o *RequestOptions) {
		o.ProtocolVersion = v
	}
}

// DirectRequest force the request to be a direct request
func DirectRequest() RequestOption {
	return func(o *RequestOptions) {
		o.RequestType = "direct_request"
	}
}

// BroadcastRequest for the request to be a broadcast mode
//
// **NOTE:** You need to ensure you have filters etc done
func BroadcastRequest() RequestOption {
	return func(o *RequestOptions) {
		o.RequestType = "request"
	}
}

// WithWorkers configures the amount of workers used to process responses
func WithWorkers(w int) RequestOption {
	return func(o *RequestOptions) {
		o.Workers = w
	}
}

// InCollective sets the collective to target a message at
func InCollective(c string) RequestOption {
	return func(o *RequestOptions) {
		o.Collective = c
	}
}

// WithReplyTo sets a custom reply to, else the connector will determine it
func WithReplyTo(r string) RequestOption {
	return func(o *RequestOptions) {
		o.ReplyTo = r
		o.ReceiveReplies = false
	}
}

// InBatches performs requests in batches
func InBatches(size int, sleep int) RequestOption {
	return func(o *RequestOptions) {
		o.Batched = true
		o.BatchSize = size
		o.BatchSleep = time.Second * time.Duration(sleep)
	}
}

// WithReplies creates a custom channel for replies and will avoid processing them
func WithReplies(r chan *choria.ConnectorMessage) RequestOption {
	return func(o *RequestOptions) {
		o.Replies = r
		o.ProcessReplies = false
	}
}

// WithTimeout configures the request timeout
func WithTimeout(t time.Duration) RequestOption {
	return func(o *RequestOptions) {
		o.TimeOut = t
	}
}

// WithReplyHandler configures a callback to be called for each message received
func WithReplyHandler(f func(protocol.Reply, *RPCReply)) RequestOption {
	return func(o *RequestOptions) {
		o.Handler = f
	}
}
