package rueidis

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"go.uber.org/goleak"
)

func TestSingleFlight(t *testing.T) {
	defer goleak.VerifyNone(t)
	var calls, done, err int64

	sg := call{}

	for i := 0; i < 1000; i++ {
		go func() {

			if ret := sg.Do(func() error {
				atomic.AddInt64(&calls, 1)
				// wait for all goroutine invoked then return
				for sg.suppressing() != 1000 {
					runtime.Gosched()
				}
				return errors.New("I should be the only ret")
			}); ret != nil {
				atomic.AddInt64(&err, 1)
			}

			atomic.AddInt64(&done, 1)
		}()
	}

	for atomic.LoadInt64(&done) != 1000 {
		runtime.Gosched()
	}

	if atomic.LoadInt64(&calls) == 0 {
		t.Fatalf("singleflight not call at all")
	}

	if v := atomic.LoadInt64(&calls); v != 1 {
		t.Fatalf("singleflight should suppress all concurrent calls, got: %v", v)
	}

	if atomic.LoadInt64(&err) != 1 {
		t.Fatalf("singleflight should that one call get the return value")
	}
}
