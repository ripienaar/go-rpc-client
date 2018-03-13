package rpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/choria-io/go-choria/choria"
	"github.com/choria-io/go-choria/mcorpc"
	log "github.com/sirupsen/logrus"
)

type RPCRequest struct {
	Agent  string          `json:"agent"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

type RPCReply struct {
	Statuscode mcorpc.StatusCode `json:"statuscode"`
	Statusmsg  string            `json:"statusmsg"`
	Data       json.RawMessage   `json:"data"`
}

// ClientConnector is a connector used for publishing and receiving replies
type ClientConnector interface {
	choria.ClientConnector
	QueueSubscribe(ctx context.Context, name string, subject string, group string, output chan *choria.ConnectorMessage) error
	ConnectedServer() string
	Close()
}

// Client is a MCollective RPC Client
type Client struct {
	fw *choria.Framework
}

// New creates an initialized RPC Client
func New(fw *choria.Framework) (*Client, error) {
	c := &Client{fw: fw}

	return c, nil
}

// NewRPCRequest constructs a message containing a RPC request for a specific agent and action
func (c *Client) NewRPCRequest(agent string, action string, payload json.RawMessage) (*choria.Message, error) {
	rpcreq := RPCRequest{
		Agent:  agent,
		Action: action,
		Data:   payload,
	}

	p, err := json.Marshal(rpcreq)
	if err != nil {
		return nil, err
	}

	msg, err := c.fw.NewMessage(string(p), agent, c.fw.Config.MainCollective, "request", nil)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// ParseReplyData parses reply data and populates a Reply and custom Data
func ParseReplyData(source []byte) (*RPCReply, error) {
	reply := &RPCReply{}

	err := json.Unmarshal(source, reply)
	if err != nil {
		return reply, fmt.Errorf("could not decode source data: %s", err)
	}

	return reply, nil
}

func (c *Client) connect(ctx context.Context, name string) (ClientConnector, error) {
	servers := func() ([]choria.Server, error) {
		return c.fw.MiddlewareServers()
	}

	var connector ClientConnector

	connector, err := c.fw.NewConnector(ctx, servers, name, log.WithFields(log.Fields{"client": true, "name": name}))
	if err != nil {
		return nil, fmt.Errorf("could not create connector: %s", err)
	}

	closer := func() {
		select {
		case <-ctx.Done():
			log.Debugf("Disconnecting from %s", connector.ConnectedServer())
			connector.Close()
		}
	}

	go closer()

	return connector, nil
}
