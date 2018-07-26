package worker

import (
	"io"
	"os/exec"
	"sync/atomic"
)

// proc represents a worker process.
type proc struct {
	cmd  *exec.Cmd
	in   io.WriteCloser
	out  io.ReadCloser
	err  io.ReadCloser
	stop chan struct{}
	wait chan error
	done int32 // atomic
}

func newProc(path string, args, env []string, dir string) *proc {
	cmd := exec.Command(path, args...)
	cmd.Env = env
	cmd.Dir = dir
	return &proc{
		cmd:  cmd,
		stop: make(chan struct{}),
		wait: make(chan error),
	}
}

// Start starts the worker process.
func (p *proc) Start() (err error) {
	p.in, err = p.cmd.StdinPipe()
	if err != nil {
		return err
	}
	p.out, err = p.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	p.err, err = p.cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := p.cmd.Start(); err != nil {
		return err
	}
	return nil
}

var testProcRunHook func(*proc) // for testing

// Run starts processing tasks from the channel and
// blocks until the tasks channel is closed or Exit
// is called.
func (p *proc) Run(tasks <-chan work) {
	defer p.exit()
	for {
		select {
		case <-p.stop:
			return
		case w, ok := <-tasks:
			if !ok {
				return
			}
			if testProcRunHook != nil {
				testProcRunHook(p)
			}
			w.err <- w.task(p.in, p.out, p.err)
		}
	}
}

var testProcExitHook func(*proc) // for testing

func (p *proc) exit() {
	if testProcExitHook != nil {
		testProcExitHook(p)
	}
	p.wait <- p.cmd.Wait()
	atomic.StoreInt32(&p.done, 1)
}

// Wait waits for the worker to exit and returns any
// errors. If a task currently runs, it will finish first.
func (p *proc) Wait() error {
	if atomic.LoadInt32(&p.done) == 1 {
		panic("Wait called twice")
	}
	close(p.stop) // stop task processing
	return <-p.wait
}

// Proc represents a worker process.
type Proc struct {
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
	proc *proc
}

var _ Worker = (*Proc)(nil)

// Start starts the worker process.
func (p *Proc) Start() error {
	p.start()
	p.proc = newProc(p.Path, p.Args, p.Env, p.Dir)
	err := p.proc.Start()
	go p.proc.Run(p.tasks)
	return err
}

// Stop stops the worker.
func (p *Proc) Stop() error {
	p.stop()
	return p.proc.Wait()
}

// CloseStdin closes stdin for all worker processes.
// This operation will temporary interrupt task execution.
func (p *Proc) CloseStdin() error {
	p.suspend()
	err := p.proc.in.Close()
	p.resume()
	return err
}

// CloseStdout closes stdout for all worker processes.
// This operation will temporary interrupt task execution.
func (p *Proc) CloseStdout() error {
	p.suspend()
	err := p.proc.out.Close()
	p.resume()
	return err
}

// CloseStderr closes stderr for all worker processes.
// This operation will temporary interrupt task execution.
func (p *Proc) CloseStderr() error {
	p.suspend()
	err := p.proc.err.Close()
	p.resume()
	return err
}
