package main

import (
	"fmt"
	"sync"
	"time"
)

func CircuitBreaker() {
	cb := newCircuitBreaker(3)

	failingOp := func() error {
		return fmt.Errorf("operation failed")
	}

	successOp := func() error {
		return nil
	}
	result := []int{1, 1, 1, 1, 0, 1, 0, 0, 1, 1, 0}

	for i := 0; i < len(result); i++ {
		var err error
		switch result[i] {
		case 1:
			err = cb.Execute(failingOp)
		case 0:
			err = cb.Execute(successOp)
		}
		if err != nil {
			fmt.Printf("Operation %d: %v\n", i, err)
		} else {
			fmt.Printf("Operation %d: operation successful\n", i)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cb.Reset()
	err := cb.Execute(successOp)
	if err != nil {
		fmt.Printf("Success operation: %v\n", err)
	}
}

type circuitBreakerState int

const (
	StateClosed circuitBreakerState = iota
	StateOpen
	StateHalfOpen
)

type circuitBreaker struct {
	threshold   int
	failures    int
	state       circuitBreakerState
	openUntil   time.Time
	openTimeout time.Duration
	mu          *sync.Mutex
}

func newCircuitBreaker(threshold int) *circuitBreaker {
	return &circuitBreaker{
		threshold:   threshold,
		openTimeout: 200 * time.Millisecond,
		mu:          &sync.Mutex{},
		state:       StateClosed,
	}
}

func (cb *circuitBreaker) Execute(operation func() error) error {
	cb.mu.Lock()
	// check if the circuit breaker should be open
	if cb.failures >= cb.threshold && cb.state != StateHalfOpen {
		// check if the open timeout has passed
		if time.Now().After(cb.openUntil) {
			cb.transitionToOpen()
		} else if cb.state == StateOpen {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker open")
		}
	}
	cb.mu.Unlock()

	err := operation()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		switch cb.state {
		case StateClosed:
			cb.failures++
			if cb.failures >= cb.threshold {
				cb.transitionToOpen()
			}
		case StateHalfOpen:
			cb.state = StateHalfOpen
		case StateOpen:
			// should not happen since operations are blocked while open
			cb.openUntil = time.Now().Add(cb.openTimeout)
		}
		return err
	}

	if cb.state == StateHalfOpen {
		cb.failures = 0
		cb.state = StateClosed
	} else if cb.state == StateClosed {
		cb.failures = 0
	}

	return nil
}

func (cb *circuitBreaker) transitionToOpen() {
	cb.state = StateOpen
	cb.openUntil = time.Now().Add(cb.openTimeout)
}

func (cb *circuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = StateClosed
	cb.openUntil = time.Time{}
}
