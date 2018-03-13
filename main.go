package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/choria-io/go-choria/choria"
	"github.com/choria-io/go-protocol/protocol"
	"github.com/ripienaar/go-rpc-client/rpc"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	fw     *choria.Framework
	debug  bool
	config string

	nodes      string
	batchSleep int
	batchSize  int

	timeout int

	sigs chan os.Signal

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel func()
	err    error
)

func cli() {
	app := kingpin.New("mc_execute_job", "MC Execute Job")
	app.Flag("nodes", "List of nodes to initiate jobs on").Required().ExistingFileVar(&nodes)
	// TODO
	// app.Flag("batch", "Batch size").Default("500").IntVar(&batchSize)
	// app.Flag("batch-sleep", "Time to sleep between batches").Default("1").IntVar(&batchSleep)
	app.Flag("config", "Configuration Path").Default(choria.UserConfig()).ExistingFileVar(&config)
	app.Flag("debug", "Debug Logging").Default("false").BoolVar(&debug)
	app.Flag("timeout", "How long to process nodes for").Default("0").IntVar(&timeout)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func setup() {
	wg = &sync.WaitGroup{}

	fw, err = choria.New(config)
	kingpin.FatalIfError(err, "Could not initialize Choria: %s", config, err)
	fw.SetupLogging(debug)

	if timeout == 0 {
		timeout = 20 + fw.Config.DiscoveryTimeout
	}

	signalWatch()
}

func targets() ([]string, error) {
	found := []string{}

	file, err := os.Open(nodes)
	if err != nil {
		return []string{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		found = append(found, scanner.Text())
	}

	err = scanner.Err()
	if err != nil {
		return []string{}, err
	}

	if len(found) == 0 {
		return found, fmt.Errorf("did not find any nodes in %s", nodes)
	}

	return found, nil
}

func execute() (*rpc.Stats, error) {
	client, err := rpc.New(fw)
	kingpin.FatalIfError(err, "Could not initialize Choria: %s", config, err)

	msg, err := client.NewRPCRequest("rpcutil", "ping", json.RawMessage("{}"))
	if err != nil {
		return nil, err
	}

	hosts, err := targets()
	if err != nil {
		return nil, err
	}

	result, err := client.Do(ctx, wg, msg,
		rpc.WithProgress(),
		rpc.DirectRequest(),
		rpc.InCollective(msg.Collective()),
		rpc.WithTargets(hosts),
		rpc.WithReplyHandler(func(r protocol.Reply, d *rpc.RPCReply) {
			log.Debugf("Reply: %s: %s: %v", r.SenderID(), d.Statusmsg, string(d.Data))
		}),
	)
	if err != nil {
		return nil, err
	}

	return result.Stats(), nil
}

func main() {
	cli()
	setup()

	stats, err := execute()
	kingpin.FatalIfError(err, "Could not execute: %s", err)

	pubtime, _ := stats.PublishDuration()
	totaltime, _ := stats.RequestDuration()
	ur := stats.UnexpectedResponseFrom()
	nr := stats.NoResponseFrom()

	fmt.Println("")
	fmt.Printf("  Request ID: %s\n", stats.RequestID)
	fmt.Printf(" Total Nodes: %d\n", stats.DiscoveredCount())
	fmt.Printf("   Responses: %d\n", stats.ResponsesCount())
	fmt.Printf("No Responses: %d\n", len(nr))
	fmt.Printf("  Unexpected: %d\n", len(ur))
	fmt.Printf("      Failed: %d\n", stats.FailCount())
	fmt.Printf("      Passed: %d\n", stats.OKCount())
	fmt.Printf("Publish Time: %s\n", pubtime.String())
	fmt.Printf("    Duration: %s\n", totaltime.String())

	if len(nr) > 0 {
		fmt.Println("")
		fmt.Println("No Responses:")

		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.AlignRight)
		choria.SliceGroups(nr, 3, func(n []string) {
			fmt.Fprintf(tw, "%s\t\n", strings.Join(n, "\t"))
		})
		tw.Flush()
		fmt.Println("")
	}

	if len(ur) > 0 {
		fmt.Println("")
		fmt.Println("Unexpected Responses:")

		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.AlignRight)
		choria.SliceGroups(ur, 3, func(n []string) {
			fmt.Fprintf(tw, "%s\t\n", strings.Join(n, "\t"))
		})
		tw.Flush()
		fmt.Println("")
	}

	os.Exit(0)
}

func signalWatch() {
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)

	watcher := func() {
		sigs = make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		select {
		case <-ctx.Done():
		case <-sigs:
			log.Warn("Shutting down on interrupt")
			cancel()
		}
	}

	go watcher()
}
