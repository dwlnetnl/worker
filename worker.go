// Package worker implements a process worker
// and process worker pool.
package worker

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// Worker represents a worker process that
// can perform a task.
type Worker interface {
	// Start starts the worker process.
	Start() error

	// Run runs a task in the worker process.
	// It blocks until the task is completed.
	Run(Task) error

	// Stop stops the worker process.
	Stop() error

	// CloseStdin closes the stdin of the worker. This
	// operation will temporary interrupt task execution.
	CloseStdin() error

	// CloseStdin closes the stdout of the worker. This
	// operation will temporary interrupt task execution.
	CloseStdout() error

	// CloseStdin closes the stderr of the worker. This
	// operation will temporary interrupt task execution.
	CloseStderr() error
}

// A Task provides access to the I/O of a worker process.
type Task func(in io.Writer, out, err io.Reader) error

type work struct {
	task Task
	err  chan error
}

type common struct {
	tasks chan work
	state int32 // atomic
	pause sync.WaitGroup

	// closed is a bit field storing which process
	// pipes are closed
	closed int32 // atomic
}

const (
	stateZero int32 = iota
	stateStart
	stateStop
)

const (
	closedIn int32 = 1 << iota
	closedOut
	closedErr
)

func (c *common) start() {
	if atomic.LoadInt32(&c.state) != stateZero {
		panic("start called twice")
	}
	atomic.StoreInt32(&c.state, stateStart)
	c.tasks = make(chan work)
}

func (c *common) stop() {
	if atomic.LoadInt32(&c.state) == stateStop {
		panic("stop called twice")
	}
	atomic.StoreInt32(&c.state, stateStop)
	close(c.tasks)
}

// Run runs a task in the context of a worker process.
// If the task returns an error, it will be returned
// from Run to the caller. Run blocks till the task is
// executed.
func (c *common) Run(t Task) error {
	if atomic.LoadInt32(&c.state) != stateStart {
		return errors.New("worker: not running")
	}
	c.pause.Wait()
	w := work{t, make(chan error, 1)}
	c.tasks <- w
	return <-w.err
}

func (c *common) suspend() { c.pause.Add(1) }
func (c *common) resume()  { c.pause.Done() }

func (c *common) isPipeClosed(pipe int32) bool {
	closed := atomic.LoadInt32(&c.closed)
	return closed&pipe != 0
}
