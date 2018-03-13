package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/choria-io/go-choria/choria"
	"github.com/choria-io/go-choria/mcorpc"
	"github.com/choria-io/go-protocol/protocol"
	log "github.com/sirupsen/logrus"
)

// RequestResult is the result of a request
type RequestResult interface {
	Stats() *Stats
}

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

// Do performs a RPC request for a message governed by options opts
func (c *Client) Do(ctx context.Context, wg *sync.WaitGroup, msg *choria.Message, opts ...RequestOption) (RequestResult, error) {
	options := &RequestOptions{
		ProtocolVersion: protocol.RequestV1,
		RequestType:     "request",
		Collective:      c.fw.Config.MainCollective,
		ProcessReplies:  true,
		ReceiveReplies:  true,
		TimeOut:         time.Duration(c.fw.Config.DiscoveryTimeout+20) * time.Second,
		stats:           NewStats(),
		wg:              wg,
		mu:              &sync.Mutex{},
	}

	options.ctx, options.cancel = context.WithCancel(ctx)

	for _, o := range opts {
		o(options)
	}

	options.stats.Start()
	defer options.stats.End()

	// https://github.com/choria-io/go-choria/issues/214
	err := c.connectPublisher(options)
	if err != nil {
		return nil, err
	}

	c.configureMessage(msg, options)

	if options.Workers == 0 {
		options.Workers = 3
	}

	options.ReceiverReady = make(chan interface{}, options.Workers)

	c.startReceivers(options)

	options.wg.Add(1)
	go c.publish(msg, options)

	options.wg.Wait()

	options.stats.End()
	options.Close()

	return options, nil
}

func (c *Client) publish(msg *choria.Message, options *RequestOptions) error {
	defer options.wg.Done()

	log.Debugf("Publisher waiting for receivers")
	select {
	case <-options.ReceiverReady:
	case <-options.ctx.Done():
		log.Warn("Could not publish, context interrupted process before any messages were published")
		return nil
	}

	log.Debugf("Starting to publish to %d targets", len(options.Targets))

	options.stats.StartPublish()
	defer options.stats.EndPublish()

	// TODO needs context https://github.com/choria-io/go-choria/issues/211
	err := options.Publisher.Publish(msg)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (c *Client) configureMessage(msg *choria.Message, options *RequestOptions) {
	if len(options.Targets) > 0 {
		msg.DiscoveredHosts = options.Targets
	}

	msg.SetProtocolVersion(options.ProtocolVersion)
	msg.SetType(options.RequestType)

	stdtarget := options.Publisher.ReplyTarget(msg)
	if options.ReplyTo == "" {
		options.ReplyTo = stdtarget
	}

	// the reply target is such that we'd probably not receive replies
	// so disable processing replies
	if stdtarget != options.ReplyTo {
		options.ReceiveReplies = false
	}

	log.Debugf("Setting reply to for %s: %s", msg.RequestID, options.ReplyTo)
	msg.SetReplyTo(options.ReplyTo)

	options.stats.SetDiscoveredNodes(options.Targets)
	options.stats.RequestID = msg.RequestID
	options.RequestID = msg.RequestID
}

func (c *Client) startReceivers(options *RequestOptions) error {
	if options.Replies == nil {
		options.Replies = make(chan *choria.ConnectorMessage, len(options.Targets))
	}

	if !options.ReceiveReplies {
		options.ReceiverReady <- nil
		return nil
	}

	log.Debugf("Starting %d receivers", options.Workers)
	for i := 0; i < options.Workers; i++ {
		options.wg.Add(1)
		go c.receiver(i, options)
	}

	return nil
}

func (c *Client) receiver(w int, options *RequestOptions) {
	defer options.wg.Done()

	name := fmt.Sprintf("%s_client_receiver_%d_%s", c.fw.Certname(), w, options.RequestID)

	connector, err := c.connect(options.ctx, name)
	if err != nil {
		log.Errorf("Receiver %d could not start: %s", err)
		return
	}

	connector.QueueSubscribe(options.ctx, "replies", options.ReplyTo, options.RequestID, options.Replies)

	log.Debugf("Worker %d notifying that we're ready", w)
	options.ReceiverReady <- nil
	log.Debugf("Worker %d notified that we're ready", w)

	if !options.ProcessReplies {
		return
	}

	if options.Progress {
		options.stats.StartProgress(options.ctx)
	}

	timeout := time.After(options.TimeOut)

	for {
		select {
		case rawmsg := <-options.Replies:
			reply, err := c.fw.NewReplyFromTransportJSON(rawmsg.Data)
			if err != nil {
				options.stats.FailedRequestInc()
				log.Errorf("Could not process a reply: %s", err)
				continue
			}
			options.stats.RecordReceived(reply.SenderID())

			r, err := ParseReplyData([]byte(reply.Message()))
			if err != nil {
				options.stats.FailedRequestInc()
				log.Errorf("Could not process reply from %s: %s", reply.SenderID(), err)
				continue
			}

			if r.Statuscode == mcorpc.OK {
				options.stats.PassedRequestInc()
			} else {
				options.stats.FailedRequestInc()
			}

			if options.Handler != nil {
				options.Handler(reply, r)
			}

			if options.stats.All() {
				options.cancel()
				return
			}

		case <-options.ctx.Done():
			return
		case <-timeout:
			log.Warnf("Timing out request %s after %s", options.RequestID, options.TimeOut.String())
			return
		}
	}
}

func (c *Client) connectPublisher(options *RequestOptions) error {
	var err error

	options.Publisher, err = c.connect(options.ctx, fmt.Sprintf("%s_publisher_%s", c.fw.Certname(), options.RequestID))
	if err != nil {
		return err
	}

	return err
}
