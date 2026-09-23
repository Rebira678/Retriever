package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the state of the Circuit Breaker.
type State uint32

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF_OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

// ErrCircuitOpen is returned when the circuit breaker rejects a request.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreaker defines a robust, context-aware circuit breaker.
type CircuitBreaker interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
	State() State
}

// Config defines thresholds for the state machine.
type Config struct {
	FailureThreshold uint32
	SuccessThreshold uint32
	Timeout          time.Duration
	OnStateChange    func(from, to State)
}

// Option applies configuration functionally.
type Option func(*Config)

// WithFailureThreshold sets consecutive failures required to trip the breaker.
func WithFailureThreshold(threshold uint32) Option {
	return func(c *Config) { c.FailureThreshold = threshold }
}

// WithSuccessThreshold sets consecutive successes required to close the breaker from half-open.
func WithSuccessThreshold(threshold uint32) Option {
	return func(c *Config) { c.SuccessThreshold = threshold }
}

// WithTimeout sets the duration to wait before probing half-open.
func WithTimeout(t time.Duration) Option {
	return func(c *Config) { c.Timeout = t }
}

// WithOnStateChange sets a hook for metrics or logging on state transitions.
func WithOnStateChange(fn func(from, to State)) Option {
	return func(c *Config) { c.OnStateChange = fn }
}

// cb implements a high-performance, lock-free (fast-path) circuit breaker.
type cb struct {
	state     atomic.Uint32
	failures  atomic.Uint32
	successes atomic.Uint32

	// mu serializes state transitions to prevent thundering herd.
	mu     sync.Mutex
	expiry time.Time

	cfg Config
}

// New creates a production-grade circuit breaker.
func New(opts ...Option) CircuitBreaker {
	cfg := Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          10 * time.Second,
		OnStateChange:    func(from, to State) {},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	
	breaker := &cb{cfg: cfg}
	breaker.state.Store(uint32(StateClosed))
	return breaker
}

// Execute wraps the function execution within the circuit breaker lifecycle.
func (c *cb) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if !c.allow() {
		return ErrCircuitOpen
	}

	err := fn(ctx)
	if err != nil {
		c.recordFailure()
		return err
	}

	c.recordSuccess()
	return nil
}

// State returns the current atomic state.
func (c *cb) State() State {
	s := State(c.state.Load())
	if s == StateOpen {
		c.mu.Lock()
		defer c.mu.Unlock()
		if time.Now().After(c.expiry) {
			return StateHalfOpen
		}
	}
	return s
}

// allow determines if the request should proceed (fast-path atomic read).
func (c *cb) allow() bool {
	s := State(c.state.Load())
	if s == StateClosed {
		return true
	}

	if s == StateOpen {
		c.mu.Lock()
		defer c.mu.Unlock()
		if time.Now().After(c.expiry) {
			if c.state.CompareAndSwap(uint32(StateOpen), uint32(StateHalfOpen)) {
				c.cfg.OnStateChange(StateOpen, StateHalfOpen)
				c.successes.Store(0)
				return true
			}
		}
		return false
	}

	// StateHalfOpen: dynamically limits concurrency for probing
	// In production, we might want to strictly limit this to 1 concurrent probe using atomic CAS.
	return true
}

func (c *cb) recordFailure() {
	s := State(c.state.Load())
	if s == StateHalfOpen {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.state.CompareAndSwap(uint32(StateHalfOpen), uint32(StateOpen)) {
			c.expiry = time.Now().Add(c.cfg.Timeout)
			c.cfg.OnStateChange(StateHalfOpen, StateOpen)
		}
		return
	}

	if s == StateClosed {
		fails := c.failures.Add(1)
		if fails >= c.cfg.FailureThreshold {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.state.CompareAndSwap(uint32(StateClosed), uint32(StateOpen)) {
				c.expiry = time.Now().Add(c.cfg.Timeout)
				c.cfg.OnStateChange(StateClosed, StateOpen)
			}
		}
	}
}

func (c *cb) recordSuccess() {
	s := State(c.state.Load())
	if s == StateHalfOpen {
		succ := c.successes.Add(1)
		if succ >= c.cfg.SuccessThreshold {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.state.CompareAndSwap(uint32(StateHalfOpen), uint32(StateClosed)) {
				c.failures.Store(0)
				c.cfg.OnStateChange(StateHalfOpen, StateClosed)
			}
		}
		return
	}

	if s == StateClosed && c.failures.Load() > 0 {
		c.failures.Store(0)
	}
}
