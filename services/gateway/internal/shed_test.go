package gateway

import (
	"sync"
	"testing"
)

func TestCreateLimiterAdmitsUpToItsLimit(t *testing.T) {
	limiter := newCreateLimiter(3)

	var releases []func()
	for i := 0; i < 3; i++ {
		release, ok := limiter.acquire()
		if !ok {
			t.Fatalf("acquire %d refused below the limit", i)
		}
		releases = append(releases, release)
	}

	if _, ok := limiter.acquire(); ok {
		t.Fatal("acquire above the limit must be refused")
	}

	releases[0]()
	release, ok := limiter.acquire()
	if !ok {
		t.Fatal("a released slot must become available again")
	}
	release()
}

// A refused acquire must not consume a slot, or the gateway ratchets itself
// closed under exactly the load shedding exists to survive.
func TestCreateLimiterRefusalDoesNotConsumeASlot(t *testing.T) {
	limiter := newCreateLimiter(1)

	release, ok := limiter.acquire()
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	for i := 0; i < 100; i++ {
		if _, ok := limiter.acquire(); ok {
			t.Fatal("acquire above the limit must be refused")
		}
	}
	release()

	if _, ok := limiter.acquire(); !ok {
		t.Fatal("repeated refusals must not have consumed the slot")
	}
}

// Release is called from a defer that can run more than once on some paths;
// double-releasing must not hand out phantom capacity.
func TestCreateLimiterReleaseIsIdempotent(t *testing.T) {
	limiter := newCreateLimiter(1)

	release, _ := limiter.acquire()
	release()
	release()
	release()

	if got := limiter.currentInFlight(); got != 0 {
		t.Fatalf("in flight = %d, want 0", got)
	}
	if _, ok := limiter.acquire(); !ok {
		t.Fatal("slot should be available")
	}
	if _, ok := limiter.acquire(); ok {
		t.Fatal("double release must not have created a second slot")
	}
}

func TestCreateLimiterIsSafeUnderConcurrency(t *testing.T) {
	const limit = 8
	limiter := newCreateLimiter(limit)

	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	held := make([]func(), 0, limit)

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, ok := limiter.acquire()
			if !ok {
				return
			}
			mu.Lock()
			admitted++
			held = append(held, release)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if admitted > limit {
		t.Fatalf("admitted %d concurrently, want at most %d", admitted, limit)
	}
	for _, release := range held {
		release()
	}
	if got := limiter.currentInFlight(); got != 0 {
		t.Fatalf("in flight = %d after releasing everything, want 0", got)
	}
}

// A nil limiter is the disabled case and must never refuse.
func TestNilCreateLimiterAdmitsEverything(t *testing.T) {
	var limiter *createLimiter
	for i := 0; i < 10; i++ {
		release, ok := limiter.acquire()
		if !ok {
			t.Fatal("a disabled limiter must admit")
		}
		release()
	}
}
