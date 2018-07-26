package worker

import (
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
)

func newTestPool(size int) *Pool {
	return &Pool{
		Size: size,
		Path: os.Args[0],
		Args: []string{"-test.run=TestProcHelper"},
		Env:  []string{"GO_WANT_HELPER_PROCESS=1"},
	}
}

func runPoolTest(t *testing.T, size int, during, check func(*Pool)) {
	p := newTestPool(size)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	procErr := make(chan error)
	for i := 1; i <= 200; i++ {
		wg.Add(1)
		go func(i int) {
			err := p.Run(func(in io.Writer, out, err io.Reader) error {
				var got string
				fmt.Fprintf(in, "task%d\n", i)
				fmt.Fscanln(out, &got)
				return nil
			})
			wg.Done()
			procErr <- err
		}(i)
	}
	p.CloseStdin()
	for i := 1; i <= 200; i++ {
		wg.Add(1)
		go func(i int) {
			err := p.Run(func(in io.Writer, out, err io.Reader) error {
				var got string
				fmt.Fprintf(in, "task%d\n", i)
				fmt.Fscanln(out, &got)
				return nil
			})
			wg.Done()
			procErr <- err
		}(i)
	}
	if during != nil {
		during(p)
	}
	wg.Wait()
	select {
	default:
	case err := <-procErr:
		if err != nil {
			t.Fatal(err)
		}
	}
	if check != nil {
		check(p)
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestPoolFixed(t *testing.T) {
	runPoolTest(t, 2, nil, nil)
}

func TestPoolGrow(t *testing.T) {
	active := make(map[*proc]struct{})
	var activeMu sync.Mutex

	defer func() { testProcRunHook = nil }()
	testProcRunHook = func(p *proc) {
		activeMu.Lock()
		active[p] = struct{}{}
		activeMu.Unlock()
	}

	const newSize = 3
	runPoolTest(t, 2, func(p *Pool) {
		p.Resize(newSize)
		if curr := p.CurrentSize(); curr != newSize {
			t.Errorf("current size is %d, want: %d", curr, newSize)
		}
	}, func(p *Pool) {
		for _, w := range p.procs {
			_, exist := active[w]
			if !exist {
				used := len(active)
				total := len(p.procs)
				t.Errorf("not all workers were used (%d of %d)", used, total)
			}
		}
	})
}

func TestPoolShrink(t *testing.T) {
	exited := make(chan struct{})
	defer func() { testProcExitHook = nil }() // ensure nil
	testProcExitHook = func(p *proc) {
		close(exited)
	}
	const newSize = 2
	runPoolTest(t, 3, func(p *Pool) {
		p.Resize(newSize)
		if curr := p.CurrentSize(); curr != newSize {
			t.Errorf("current size is %d, want: %d", curr, newSize)
		}
		// We don't want to catch the regular
		// pool tear down, so set hook to nil.
		testProcExitHook = nil
	}, func(p *Pool) {
		select {
		case <-exited:
		default:
			t.Error("worker did not exit")
		}
	})
}
