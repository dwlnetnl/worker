package worker

import (
	"fmt"
	"io"
	"os"
	"testing"
)

// Instead of using separate code to implement a
// worker process, use the fact that `go test`
// runs a self-contained binary that we can call.
//
// By calling a specific test method with a special
// enviorment variable set, it behaves like a some
// external program, but with the code in the same
// file along side the code of the test.

func TestProcHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	var err error
	for err != io.EOF {
		// echo back input
		var s string
		_, err = fmt.Scanln(&s)
		fmt.Println(s)
	}
	os.Exit(0)
}

func TestProc(t *testing.T) {
	p := &Proc{
		Path: os.Args[0],
		Args: []string{"-test.run=TestProcHelper"},
		Env:  []string{"GO_WANT_HELPER_PROCESS=1"},
	}

	if err := p.Start(); err != nil {
		t.Fatal(err)
	}

	var got string
	const want = "hello"
	done := make(chan struct{})
	p.Run(func(in io.Writer, out, err io.Reader) error {
		fmt.Fprintln(in, "hello")
		fmt.Fscanln(out, &got)
		close(done)
		return nil
	})
	p.CloseStdin()
	p.CloseStdin()
	<-done
	if got != want {
		t.Errorf("got %q, want: %q", got, want)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func newTestExitProc() *proc {
	args := []string{"-test.run=TestProcExitHelper"}
	env := []string{"GO_WANT_HELPER_PROCESS=1"}
	return newProc(os.Args[0], args, env, "")
}

func TestProcExitHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	os.Exit(0)
}

func TestProcExitOrder(t *testing.T) {
	t.Run("close-exit", func(t *testing.T) {
		p := newTestExitProc()
		tasks := make(chan work)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		go p.Run(tasks)

		// Closing task channel exits the process.
		close(tasks)
		// Exit stops the process and returns the exit error.
		if err := p.Wait(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("exit-close", func(t *testing.T) {
		p := newTestExitProc()
		tasks := make(chan work)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		go p.Run(tasks)

		// Closing task channel exits the process.
		close(tasks)
		// Exit stops the process and returns the exit error.
		if err := p.Wait(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProcExitPanic(t *testing.T) {
	p := newTestExitProc()

	tasks := make(chan work)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	go p.Run(tasks)

	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if v := recover(); v == nil {
			t.Error("no panic on calling Exit twice")
		}
	}()
	p.Wait()
}
