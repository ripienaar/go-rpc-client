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

// Do performs a RPC request for a message governed by options opts
func (c *Client) Do(ctx context.Context, wg *sync.WaitGroup, msg *choria.Message, opts ...RequestOption) (RequestResult, error) {
	options := &RequestOptions{
		ProtocolVersion: protocol.RequestV1,
		RequestType:     "direct_request",
		Collective:      c.fw.Config.MainCollective,
		ProcessReplies:  true,
		ReceiveReplies:  true,
		Progress:        false,
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

	err := c.connectPublisher(options)
	if err != nil {
		return err
	}

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
	err = options.Publisher.Publish(msg)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (c *Client) configureMessage(msg *choria.Message, options *RequestOptions) {
	if len(options.Targets) > 0 {
		msg.DiscoveredHosts = options.Targets
	} else {
		options.Targets = msg.DiscoveredHosts
	}

	msg.SetProtocolVersion(options.ProtocolVersion)
	msg.SetType(options.RequestType)

	stdtarget := choria.ReplyTarget(msg, c.fw.NewRequestID())
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
