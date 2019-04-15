// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webdavredisls

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	webdav "github.com/koofr/go-webdav"
	"gopkg.in/redsync.v1"
)

func getByName(r *RedisLS, name string) *RedisLSNode {
	conn := r.pool.Get()
	defer conn.Close()

	n, err := r.getByName(conn, name)
	if err != nil {
		panic(err)
	}

	return n
}

func byNameAll(r *RedisLS) map[string]*RedisLSNode {
	conn := r.pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", r.byNameKey("*")))
	if err != nil {
		panic(err)
	}

	res := map[string]*RedisLSNode{}

	prefix := r.byNameKey("")

	for _, key := range keys {
		n, err := r.getByName(conn, key[len(prefix):])
		if err != nil {
			panic(err)
		}
		res[n.name] = n
	}

	return res
}

func byNameLen(r *RedisLS) int {
	conn := r.pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", r.byNameKey("*")))
	if err != nil {
		panic(err)
	}

	return len(keys)
}

func getByToken(r *RedisLS, token string) *RedisLSNode {
	conn := r.pool.Get()
	defer conn.Close()

	n, err := r.getByToken(conn, token)
	if err != nil {
		panic(err)
	}

	return n
}

func byTokenAll(r *RedisLS) map[string]*RedisLSNode {
	conn := r.pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", r.byTokenKey("*")))
	if err != nil {
		panic(err)
	}

	res := map[string]*RedisLSNode{}

	prefix := r.byTokenKey("")

	for _, key := range keys {
		token := key[len(prefix):]
		n, err := r.getByToken(conn, token)
		if err != nil {
			panic(err)
		}
		res[token] = n
	}

	return res
}

func byTokenLen(r *RedisLS) int {
	conn := r.pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", r.byTokenKey("*")))
	if err != nil {
		panic(err)
	}

	return len(keys)
}

func byExpiryAll(r *RedisLS) []*RedisLSNode {
	conn := r.pool.Get()
	defer conn.Close()

	names, err := redis.Strings(r.connDo(conn, "ZRANGEBYSCORE", r.expiryZSetKey, "-inf", "+inf"))
	if err != nil {
		panic(err)
	}

	res := []*RedisLSNode{}

	for _, name := range names {
		n, err := r.getByName(conn, name)
		if err != nil {
			panic(err)
		}
		if n == nil {
			panic("Inconsistent state: byExpiryAll name not found: " + name)
		}
		res = append(res, n)
	}

	return res
}

func TestWalkToRoot(t *testing.T) {
	testCases := []struct {
		name string
		want []string
	}{{
		"/a/b/c/d",
		[]string{
			"/a/b/c/d",
			"/a/b/c",
			"/a/b",
			"/a",
			"/",
		},
	}, {
		"/a",
		[]string{
			"/a",
			"/",
		},
	}, {
		"/",
		[]string{
			"/",
		},
	}}

	for _, tc := range testCases {
		var got []string
		if ok, _ := walkToRoot(tc.name, func(name0 string, first bool) (bool, error) {
			if first != (len(got) == 0) {
				t.Errorf("name=%q: first=%t but len(got)==%d", tc.name, first, len(got))
				return false, nil
			}
			got = append(got, name0)
			return true, nil
		}); !ok {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("name=%q:\ngot  %q\nwant %q", tc.name, got, tc.want)
		}
	}
}

var lockTestDurations = []time.Duration{
	infiniteTimeout, // infiniteTimeout means to never expire.
	0,               // A zero duration means to expire immediately.
	100 * time.Hour, // A very large duration will not expire in these tests.
}

// lockTestNames are the names of a set of mutually compatible locks. For each
// name fragment:
//  - _ means no explicit lock.
//  - i means a infinite-depth lock,
//  - z means a zero-depth lock,
var lockTestNames = []string{
	"/_/_/_/_/z",
	"/_/_/i",
	"/_/z",
	"/_/z/i",
	"/_/z/z",
	"/_/z/_/i",
	"/_/z/_/z",
	"/i",
	"/z",
	"/z/_/i",
	"/z/_/z",
}

func lockTestZeroDepth(name string) bool {
	switch name[len(name)-1] {
	case 'i':
		return false
	case 'z':
		return true
	}
	panic(fmt.Sprintf("lock name %q did not end with 'i' or 'z'", name))
}

func NewTestRedisLS() *RedisLS {
	server := os.Getenv("REDIS_SERVER")
	if server == "" {
		server = "localhost:6379"
	}

	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < 10*time.Second {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	prefix := "webdavredislstest:"

	rs := redsync.New([]redsync.Pool{pool})
	mutex := rs.NewMutex("webdavredislstest:mutex")

	conn := pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		_, err := conn.Do("DEL", key)
		if err != nil {
			panic(err)
		}
	}

	return NewRedisLS(pool, prefix, mutex)
}

func TestMemLSCanCreate(t *testing.T) {
	now := time.Unix(0, 0)
	r := NewTestRedisLS()
	conn := r.pool.Get()
	defer conn.Close()

	for _, name := range lockTestNames {
		_, err := r.Create(now, webdav.LockDetails{
			Root:      name,
			Duration:  infiniteTimeout,
			ZeroDepth: lockTestZeroDepth(name),
		})
		if err != nil {
			t.Fatalf("creating lock for %q: %v", name, err)
		}
	}

	wantCanCreate := func(name string, zeroDepth bool) bool {
		for _, n := range lockTestNames {
			switch {
			case n == name:
				// An existing lock has the same name as the proposed lock.
				return false
			case strings.HasPrefix(n, name):
				// An existing lock would be a child of the proposed lock,
				// which conflicts if the proposed lock has infinite depth.
				if !zeroDepth {
					return false
				}
			case strings.HasPrefix(name, n):
				// An existing lock would be an ancestor of the proposed lock,
				// which conflicts if the ancestor has infinite depth.
				if n[len(n)-1] == 'i' {
					return false
				}
			}
		}
		return true
	}

	var check func(int, string)
	check = func(recursion int, name string) {
		for _, zeroDepth := range []bool{false, true} {
			got, err := r.canCreate(conn, name, zeroDepth)
			if err != nil {
				panic(err)
			}
			want := wantCanCreate(name, zeroDepth)
			if got != want {
				t.Errorf("canCreate name=%q zeroDepth=%t: got %t, want %t", name, zeroDepth, got, want)
			}
		}
		if recursion == 6 {
			return
		}
		if name != "/" {
			name += "/"
		}
		for _, c := range "_iz" {
			check(recursion+1, name+string(c))
		}
	}
	check(0, "/")

	if err := r.consistent(); err != nil {
		t.Fatalf("TestMemLSCanCreate: inconsistent state: %v", err)
	}
}

func TestMemLSLookup(t *testing.T) {
	now := time.Unix(0, 0)
	r := NewTestRedisLS()

	conn := r.pool.Get()
	defer conn.Close()

	badToken, err := r.nextToken(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("badToken=%q", badToken)

	for _, name := range lockTestNames {
		token, err := r.Create(now, webdav.LockDetails{
			Root:      name,
			Duration:  infiniteTimeout,
			ZeroDepth: lockTestZeroDepth(name),
		})
		if err != nil {
			t.Fatalf("creating lock for %q: %v", name, err)
		}
		t.Logf("%-15q -> node=%p token=%q", name, getByName(r, name), token)
	}

	baseNames := append([]string{"/a", "/b/c"}, lockTestNames...)
	for _, baseName := range baseNames {
		for _, suffix := range []string{"", "/0", "/1/2/3"} {
			name := baseName + suffix

			goodToken := ""
			base := getByName(r, baseName)
			if base != nil && (suffix == "" || !lockTestZeroDepth(baseName)) {
				goodToken = base.token
			}

			for _, token := range []string{badToken, goodToken} {
				if token == "" {
					continue
				}

				got, err := r.lookup(conn, name, webdav.Condition{Token: token})
				if err != nil {
					panic(err)
				}
				want := base
				if token == badToken {
					want = nil
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("name=%-20qtoken=%q (bad=%t): got %p, want %p",
						name, token, token == badToken, got, want)
				}
			}
		}
	}
}

func TestMemLSConfirm(t *testing.T) {
	now := time.Unix(0, 0)
	r := NewTestRedisLS()
	alice, err := r.Create(now, webdav.LockDetails{
		Root:      "/alice",
		Duration:  infiniteTimeout,
		ZeroDepth: false,
	})
	tweedle, err := r.Create(now, webdav.LockDetails{
		Root:      "/tweedle",
		Duration:  infiniteTimeout,
		ZeroDepth: false,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Create: inconsistent state: %v", err)
	}

	// Test a mismatch between name and condition.
	_, err = r.Confirm(now, "/tweedle/dee", "", webdav.Condition{Token: alice})
	if err != webdav.ErrConfirmationFailed {
		t.Fatalf("Confirm (mismatch): got %v, want webdav.ErrConfirmationFailed", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Confirm (mismatch): inconsistent state: %v", err)
	}

	// Test two names (that fall under the same lock) in the one Confirm call.
	release, err := r.Confirm(now, "/tweedle/dee", "/tweedle/dum", webdav.Condition{Token: tweedle})
	if err != nil {
		t.Fatalf("Confirm (twins): %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Confirm (twins): inconsistent state: %v", err)
	}
	release()
	if err := r.consistent(); err != nil {
		t.Fatalf("release (twins): inconsistent state: %v", err)
	}

	// Test the same two names in overlapping Confirm / release calls.
	releaseDee, err := r.Confirm(now, "/tweedle/dee", "", webdav.Condition{Token: tweedle})
	if err != nil {
		t.Fatalf("Confirm (sequence #0): %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Confirm (sequence #0): inconsistent state: %v", err)
	}

	_, err = r.Confirm(now, "/tweedle/dum", "", webdav.Condition{Token: tweedle})
	if err != webdav.ErrConfirmationFailed {
		t.Fatalf("Confirm (sequence #1): got %v, want webdav.ErrConfirmationFailed", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Confirm (sequence #1): inconsistent state: %v", err)
	}

	releaseDee()
	if err := r.consistent(); err != nil {
		t.Fatalf("release (sequence #2): inconsistent state: %v", err)
	}

	releaseDum, err := r.Confirm(now, "/tweedle/dum", "", webdav.Condition{Token: tweedle})
	if err != nil {
		t.Fatalf("Confirm (sequence #3): %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Confirm (sequence #3): inconsistent state: %v", err)
	}

	// Test that you can't unlock a held lock.
	err = r.Unlock(now, tweedle)
	if err != webdav.ErrLocked {
		t.Fatalf("Unlock (sequence #4): got %v, want ErrLocked", err)
	}

	releaseDum()
	if err := r.consistent(); err != nil {
		t.Fatalf("release (sequence #5): inconsistent state: %v", err)
	}

	err = r.Unlock(now, tweedle)
	if err != nil {
		t.Fatalf("Unlock (sequence #6): %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Unlock (sequence #6): inconsistent state: %v", err)
	}
}

func TestMemLSNonCanonicalRoot(t *testing.T) {
	now := time.Unix(0, 0)
	r := NewTestRedisLS()
	token, err := r.Create(now, webdav.LockDetails{
		Root:     "/foo/./bar//",
		Duration: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Create: inconsistent state: %v", err)
	}
	if err := r.Unlock(now, token); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := r.consistent(); err != nil {
		t.Fatalf("Unlock: inconsistent state: %v", err)
	}
}

func TestMemLSExpiry(t *testing.T) {
	r := NewTestRedisLS()

	conn := r.pool.Get()
	defer conn.Close()

	testCases := []string{
		"setNow 0",
		"create /a.5",
		"want /a.5",
		"create /c.6",
		"want /a.5 /c.6",
		"create /a/b.7",
		"want /a.5 /a/b.7 /c.6",
		"setNow 4",
		"want /a.5 /a/b.7 /c.6",
		"setNow 5",
		"want /a/b.7 /c.6",
		"setNow 6",
		"want /a/b.7",
		"setNow 7",
		"want ",
		"setNow 8",
		"want ",
		"create /a.12",
		"create /b.13",
		"create /c.15",
		"create /a/d.16",
		"want /a.12 /a/d.16 /b.13 /c.15",
		"refresh /a.14",
		"want /a.14 /a/d.16 /b.13 /c.15",
		"setNow 12",
		"want /a.14 /a/d.16 /b.13 /c.15",
		"setNow 13",
		"want /a.14 /a/d.16 /c.15",
		"setNow 14",
		"want /a/d.16 /c.15",
		"refresh /a/d.20",
		"refresh /c.20",
		"want /a/d.20 /c.20",
		"setNow 20",
		"want ",
	}

	tokens := map[string]string{}
	zTime := time.Unix(0, 0)
	now := zTime
	for i, tc := range testCases {
		j := strings.IndexByte(tc, ' ')
		if j < 0 {
			t.Fatalf("test case #%d %q: invalid command", i, tc)
		}
		op, arg := tc[:j], tc[j+1:]
		switch op {
		default:
			t.Fatalf("test case #%d %q: invalid operation %q", i, tc, op)

		case "create", "refresh":
			parts := strings.Split(arg, ".")
			if len(parts) != 2 {
				t.Fatalf("test case #%d %q: invalid create", i, tc)
			}
			root := parts[0]
			d, err := strconv.Atoi(parts[1])
			if err != nil {
				t.Fatalf("test case #%d %q: invalid duration", i, tc)
			}
			dur := time.Unix(0, 0).Add(time.Duration(d) * time.Second).Sub(now)

			switch op {
			case "create":
				token, err := r.Create(now, webdav.LockDetails{
					Root:      root,
					Duration:  dur,
					ZeroDepth: true,
				})
				if err != nil {
					t.Fatalf("test case #%d %q: Create: %v", i, tc, err)
				}
				tokens[root] = token

			case "refresh":
				token := tokens[root]
				if token == "" {
					t.Fatalf("test case #%d %q: no token for %q", i, tc, root)
				}
				got, err := r.Refresh(now, token, dur)
				if err != nil {
					t.Fatalf("test case #%d %q: Refresh: %v", i, tc, err)
				}
				want := webdav.LockDetails{
					Root:      root,
					Duration:  dur,
					ZeroDepth: true,
				}
				if got != want {
					t.Fatalf("test case #%d %q:\ngot  %v\nwant %v", i, tc, got, want)
				}
			}

		case "setNow":
			d, err := strconv.Atoi(arg)
			if err != nil {
				t.Fatalf("test case #%d %q: invalid duration", i, tc)
			}
			now = time.Unix(0, 0).Add(time.Duration(d) * time.Second)

		case "want":
			r.mu.Lock()
			err := r.collectExpiredNodes(conn, now)
			if err != nil {
				panic(err)
			}
			got := make([]string, 0, byTokenLen(r))
			for _, n := range byTokenAll(r) {
				got = append(got, fmt.Sprintf("%s.%d",
					n.details.Root, n.expiry.Sub(zTime)/time.Second))
			}
			r.mu.Unlock()
			sort.Strings(got)
			want := []string{}
			if arg != "" {
				want = strings.Split(arg, " ")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("test case #%d %q:\ngot  %q\nwant %q", i, tc, got, want)
			}
		}

		if err := r.consistent(); err != nil {
			t.Fatalf("test case #%d %q: inconsistent state: %v", i, tc, err)
		}
	}
}

func TestMemLS(t *testing.T) {
	now := time.Unix(0, 0)
	r := NewTestRedisLS()
	rng := rand.New(rand.NewSource(0))
	tokens := map[string]string{}
	nConfirm, nCreate, nRefresh, nUnlock := 0, 0, 0, 0
	const N = 2000

	for i := 0; i < N; i++ {
		name := lockTestNames[rng.Intn(len(lockTestNames))]
		duration := lockTestDurations[rng.Intn(len(lockTestDurations))]
		confirmed, unlocked := false, false

		// If the name was already locked, we randomly confirm/release, refresh
		// or unlock it. Otherwise, we create a lock.
		token := tokens[name]
		if token != "" {
			switch rng.Intn(3) {
			case 0:
				confirmed = true
				nConfirm++
				release, err := r.Confirm(now, name, "", webdav.Condition{Token: token})
				if err != nil {
					t.Fatalf("iteration #%d: Confirm %q: %v", i, name, err)
				}
				if err := r.consistent(); err != nil {
					t.Fatalf("iteration #%d: inconsistent state: %v", i, err)
				}
				release()

			case 1:
				nRefresh++
				if _, err := r.Refresh(now, token, duration); err != nil {
					t.Fatalf("iteration #%d: Refresh %q: %v", i, name, err)
				}

			case 2:
				unlocked = true
				nUnlock++
				if err := r.Unlock(now, token); err != nil {
					t.Fatalf("iteration #%d: Unlock %q: %v", i, name, err)
				}
			}

		} else {
			nCreate++
			var err error
			token, err = r.Create(now, webdav.LockDetails{
				Root:      name,
				Duration:  duration,
				ZeroDepth: lockTestZeroDepth(name),
			})
			if err != nil {
				t.Fatalf("iteration #%d: Create %q: %v", i, name, err)
			}
		}

		if !confirmed {
			if duration == 0 || unlocked {
				// A zero-duration lock should expire immediately and is
				// effectively equivalent to being unlocked.
				tokens[name] = ""
			} else {
				tokens[name] = token
			}
		}

		if err := r.consistent(); err != nil {
			t.Fatalf("iteration #%d: inconsistent state: %v", i, err)
		}
	}

	if nConfirm < N/10 {
		t.Fatalf("too few Confirm calls: got %d, want >= %d", nConfirm, N/10)
	}
	if nCreate < N/10 {
		t.Fatalf("too few Create calls: got %d, want >= %d", nCreate, N/10)
	}
	if nRefresh < N/10 {
		t.Fatalf("too few Refresh calls: got %d, want >= %d", nRefresh, N/10)
	}
	if nUnlock < N/10 {
		t.Fatalf("too few Unlock calls: got %d, want >= %d", nUnlock, N/10)
	}
}

func (r *RedisLS) consistent() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If r.byName is non-empty, then it must contain an entry for the root "/",
	// and its refCount should equal the number of locked nodes.
	if byNameLen(r) > 0 {
		n := getByName(r, "/")
		if n == nil {
			return fmt.Errorf(`non-empty r.byName does not contain the root "/"`)
		}
		if n.refCount != byTokenLen(r) {
			return fmt.Errorf("root node refCount=%d, differs from byTokenLen(r)=%d", n.refCount, byTokenLen(r))
		}
	}

	for name, n := range byNameAll(r) {
		// The map keys should be consistent with the node's copy of the key.
		if n.details.Root != name {
			return fmt.Errorf("node name %q != byName map key %q", n.details.Root, name)
		}

		// A name must be clean, and start with a "/".
		if len(name) == 0 || name[0] != '/' {
			return fmt.Errorf(`node name %q does not start with "/"`, name)
		}
		if name != path.Clean(name) {
			return fmt.Errorf(`node name %q is not clean`, name)
		}

		// A node's refCount should be positive.
		if n.refCount <= 0 {
			return fmt.Errorf("non-positive refCount for node at name %q", name)
		}

		// A node's refCount should be the number of self-or-descendents that
		// are locked (i.e. have a non-empty token).
		var list []string
		for name0, n0 := range byNameAll(r) {
			// All of lockTestNames' name fragments are one byte long: '_', 'i' or 'z',
			// so strings.HasPrefix is equivalent to self-or-descendent name match.
			// We don't have to worry about "/foo/bar" being a false positive match
			// for "/foo/b".
			if strings.HasPrefix(name0, name) && n0.token != "" {
				list = append(list, name0)
			}
		}
		if n.refCount != len(list) {
			sort.Strings(list)
			return fmt.Errorf("node at name %q has refCount %d but locked self-or-descendents are %q (len=%d)",
				name, n.refCount, list, len(list))
		}

		// A node n is in r.byToken if it has a non-empty token.
		if n.token != "" {
			if nn := getByToken(r, n.token); nn == nil {
				return fmt.Errorf("node at name %q has token %q but not in byToken", name, n.token)
			}
		}
	}

	for token, n := range byTokenAll(r) {
		// The map keys should be consistent with the node's copy of the key.
		if n.token != token {
			return fmt.Errorf("node token %q != byToken map key %q", n.token, token)
		}

		// Every node in r.byToken is in r.byName.
		if n := getByName(r, n.details.Root); n == nil {
			return fmt.Errorf("node at name %q in byToken but not in byName", n.details.Root)
		}
	}

	for _, n := range byExpiryAll(r) {
		// Every node in byExpiry is in byName.
		if nn := getByName(r, n.details.Root); nn == nil {
			return fmt.Errorf("node at name %q in byExpiry but not in byName", n.details.Root)
		}

		// No node in byExpiry should be held.
		if n.held {
			return fmt.Errorf("node at name %q in byExpiry is held", n.details.Root)
		}
	}

	return nil
}
