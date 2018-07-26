package worker

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Pool is a pool of worker processes.
type Pool struct {
	// Size specifies the initial size of the pool.
	// Call CurrentSize() to get the actual size.
	Size int

	// Path is the path of the command to run.
	//
	// This is the only field that must be set to a non-zero
	// value. If Path is relative, it is evaluated relative
	// to Dir.
	Path string

	// Args holds command line arguments.
	Args []string

	// Env specifies the environment of the process. Each
	// entry is of the form "key=value". If Env is nil,
	// the new process uses the current process's
	// environment. If Env contains duplicate environment
	// keys, only the last value in the slice for each
	// duplicate key is used.
	Env []string

	// Dir specifies the working directory of the command.
	// If Dir is the empty string, the command runs in the
	// calling process's current directory.
	Dir string

	common
	procs []*proc

	// procsMu protects the procs slice, not the procs
	// themselves. If a pipe is closed, procsMu must be
	// locked as well to prevent a race between closing
	// the pipe and growing the procs.
	procsMu sync.Mutex
}

var _ Worker = (*Pool)(nil)

func assertSize(size int) {
	if size <= 0 {
		panic(fmt.Sprintf("pool size is invalid (%d)", size))
	}
}

// Start starts the workers in the pool. It returns
// an error if a worker cannot be started.
func (p *Pool) Start() error {
	assertSize(p.Size)
	p.start()
	p.procs = make([]*proc, p.Size)
	for i := 0; i < p.Size; i++ {
		w := newProc(p.Path, p.Args, p.Env, p.Dir)
		err := w.Start()
		if err != nil {
			curr := i
			// stop workers that are already started
			for i := 0; i < curr; i++ {
				w.Wait()
			}
			return err
		}
		go w.Run(p.tasks)
		p.procs[i] = w
	}
	return nil
}

// Stop stops the workers in the pool and returns if
// there is any error. Only the first worker error
// encountered is returned.
func (p *Pool) Stop() (err error) {
	p.stop()

	// No procs lock required, only operating on elements.
	for _, w := range p.procs {
		werr := w.Wait()
		if werr != nil && err == nil {
			err = werr
		}
	}
	return err
}

// CloseStdin closes stdin for all worker processes.
// This operation will temporary interrupt task execution.
func (p *Pool) CloseStdin() (err error) {
	return p.closePipe(closedIn, func(p *proc) io.Closer {
		return p.in
	})
}

// CloseStdout closes stdout for all worker processes.
// This operation will temporary interrupt task execution.
func (p *Pool) CloseStdout() (err error) {
	return p.closePipe(closedOut, func(p *proc) io.Closer {
		return p.out
	})
}

// CloseStderr closes stderr for all worker processes.
// This operation will temporary interrupt task execution.
func (p *Pool) CloseStderr() error {
	return p.closePipe(closedErr, func(p *proc) io.Closer {
		return p.err
	})
}

func (p *Pool) closePipe(pipe int32, closer func(*proc) io.Closer) error {
	if p.isPipeClosed(pipe) {
		return nil
	}
	// We have to lock procsMu as well because closing
	// the pipe and growing the pool simultaneously
	// means trouble.
	p.procsMu.Lock()
	atomic.AddInt32(&p.closed, pipe)
	p.suspend()
	var err error
	for _, w := range p.procs {
		c := closer(w)
		cerr := c.Close()
		if cerr != nil && err == nil {
			err = cerr
		}
	}
	p.resume()
	p.procsMu.Unlock()
	return err
}

// CurrentSize returns the current size of the pool.
// This may differ from the initial size because of
// resizes that might have happend.
func (p *Pool) CurrentSize() int {
	p.procsMu.Lock()
	curr := len(p.procs)
	p.procsMu.Unlock()
	return curr
}

// Resize resizes the worker pool dynamically to the
// given size. It panics if size < 0. In case of an
// error, only the first is returned.
func (p *Pool) Resize(size int) (err error) {
	assertSize(size)
	p.procsMu.Lock()
	curr := len(p.procs)
	if curr > size {
		return p.shrinkProcsUnlocks(curr - size)
	}
	return p.growProcsUnlocks(size - curr)
}

func (p *Pool) growProcsUnlocks(n int) error {
	// these won't change because procsMu is locked
	closed := atomic.LoadInt32(&p.closed)

	// Start new workers first and return
	// early in case of an error.
	created := make([]*proc, 0, n)
	for i := 0; i < n; i++ {
		w := newProc(p.Path, p.Args, p.Env, p.Dir)
		err := w.Start()
		if err != nil {
			// stop newly created workers
			for _, w := range created {
				w.Wait()
			}
			return err
		}
		if closed&closedIn != 0 {
			w.in.Close()
		}
		if closed&closedOut != 0 {
			w.out.Close()
		}
		if closed&closedErr != 0 {
			w.err.Close()
		}
		created = append(created, w)
	}

	// add new workers to the worker pool
	p.procs = append(p.procs, created...)

	// no need to touch p.procs anymore
	p.procsMu.Unlock()

	// start processing tasks in new workers
	for _, w := range created {
		go w.Run(p.tasks)
	}

	return nil
}

func (p *Pool) shrinkProcsUnlocks(n int) (err error) {
	// remove excess workers from pool
	curr := len(p.procs)
	excess := p.procs[curr-n:]
	p.procs = p.procs[:curr-n]

	// no need to touch p.procs anymore
	p.procsMu.Unlock()

	// exit excess workers
	for _, w := range excess {
		werr := w.Wait()
		if werr != nil && err == nil {
			err = werr
		}
	}

	return err
}
