package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/robustperception/pushprox/util"
)

var (
	registrationTimeout = kingpin.Flag("registration.timeout", "After how long a registration expires.").Default("5m").Duration()
)

type client struct {
	time.Time
	labels map[string]string
}

type Coordinator struct {
	mu sync.Mutex

	// Clients waiting for a scrape.
	waiting map[string]chan *http.Request
	// Responses from clients.
	responses map[string]chan *http.Response
	// Clients we know about and when they last contacted us.
	known map[string]client

	logger log.Logger
}

func NewCoordinator(logger log.Logger) *Coordinator {
	c := &Coordinator{
		waiting:   map[string]chan *http.Request{},
		responses: map[string]chan *http.Response{},
		known:     map[string]client{},
		logger:    logger,
	}
	go c.gc()
	return c
}

var idCounter int64

// Generate a unique ID
func genId() string {
	id := atomic.AddInt64(&idCounter, 1)
	// TODO: Add MAC address.
	// TODO: Sign these to prevent spoofing.
	return fmt.Sprintf("%d-%d-%d", time.Now().Unix(), id, os.Getpid())
}

func (c *Coordinator) getRequestChannel(fqdn string) chan *http.Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.waiting[fqdn]
	if !ok {
		ch = make(chan *http.Request)
		c.waiting[fqdn] = ch
	}
	return ch
}

func (c *Coordinator) getResponseChannel(id string) chan *http.Response {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.responses[id]
	if !ok {
		ch = make(chan *http.Response)
		c.responses[id] = ch
	}
	return ch
}

// Remove a response channel. Idempotent.
func (c *Coordinator) removeResponseChannel(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.responses, id)
}

// Request a scrape.
func (c *Coordinator) DoScrape(ctx context.Context, r *http.Request) (*http.Response, error) {
	id := genId()
	level.Info(c.logger).Log("msg", "DoScrape", "scrape_id", id, "url", r.URL.String())
	r.Header.Add("Id", id)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.getRequestChannel(r.URL.Hostname()) <- r:
	}

	respCh := c.getResponseChannel(id)
	defer c.removeResponseChannel(id)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp, nil
	}
}

// Client registering to accept a scrape request. Blocking.
func (c *Coordinator) WaitForScrapeInstruction(fqdn string, labels map[string]string) (*http.Request, error) {
	level.Info(c.logger).Log("msg", "WaitForScrapeInstruction", "fqdn, labels", fqdn, labels)

	if err := c.addKnownClient(fqdn, labels); err != nil {
		return nil, err
	}
	// TODO: What if the client times out?
	ch := c.getRequestChannel(fqdn)

	// exhaust existing poll request (eg. timeouted queues)
	select {
	case ch <- nil:
		//
	default:
		break
	}

	for {
		request := <-ch
		if request == nil {
			return nil, fmt.Errorf("request is expired")
		}

		select {
		case <-request.Context().Done():
			// Request has timed out, get another one.
		default:
			return request, nil
		}
	}
}

// Client sending a scrape result in.
func (c *Coordinator) ScrapeResult(r *http.Response) error {
	id := r.Header.Get("Id")
	level.Info(c.logger).Log("msg", "ScrapeResult", "scrape_id", id)
	ctx, _ := context.WithTimeout(context.Background(), util.GetScrapeTimeout(maxScrapeTimeout, defaultScrapeTimeout, r.Header))
	// Don't expose internal headers.
	r.Header.Del("Id")
	r.Header.Del("X-Prometheus-Scrape-Timeout-Seconds")
	select {
	case c.getResponseChannel(id) <- r:
		return nil
	case <-ctx.Done():
		c.removeResponseChannel(id)
		return ctx.Err()
	}
}

func (c *Coordinator) addKnownClient(fqdn string, labels map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if target preexists with different labels, error since it means
	//   hostnames are not unique, which is required by current pushprox architecture
	if clienT, ok := c.known[fqdn]; ok {
		if !reflect.DeepEqual(clienT.labels, labels) {
			return fmt.Errorf("Same FQDN, different labels! PushProx needs FQDNs to be unique across all environments")
		}
	}

	c.known[fqdn] = client{time.Now(), labels}

	return nil
}

// What clients are alive.
func (c *Coordinator) KnownClients() map[string]client {
	c.mu.Lock()
	defer c.mu.Unlock()

	limit := time.Now().Add(-*registrationTimeout)
	known := map[string]client{}
	for fqdn, client := range c.known {
		if limit.Before(client.Time) {
			known[fqdn] = client
		}
	}
	return known
}

// Garbagee collect old clients.
func (c *Coordinator) gc() {
	for range time.Tick(1 * time.Minute) {
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			limit := time.Now().Add(-*registrationTimeout)
			deleted := 0
			for fqdn, client := range c.known {
				if client.Time.Before(limit) {
					delete(c.known, fqdn)
					deleted++
				}
			}
			level.Info(c.logger).Log("msg", "GC of clients completed", "deleted", deleted, "remaining", len(c.known))
		}()
	}
}
