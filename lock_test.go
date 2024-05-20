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

	"github.com/gomodule/redigo/redis"
	webdav "github.com/koofr/go-webdav"
)

type RedisLSNode struct {
	name string
	// details are the lock metadata. Even if this node's name is not explicitly locked,
	// details.Root will still equal the node's name.
	details webdav.LockDetails
	// token is the unique identifier for this node's lock. An empty token means that
	// this node is not explicitly locked.
	token string
	// refCount is the number of self-or-descendent nodes that are explicitly locked.
	refCount int
	// expiry is when this node's lock expires.
	expiry time.Time
	// held is whether this node's lock is actively held by a Confirm call.
	held bool
}

func (r *RedisLS) redisLog(msg string) {
	// for debugging in redis-cli with MONITOR
	conn := r.pool.Get()
	conn.Do("PING", "******** RedisLS log: "+msg)
	conn.Close()
}

func (r *RedisLS) byNameKey(name string) string {
	return r.prefix + namePrefix + name
}

func (r *RedisLS) byTokenKey(name string) string {
	return r.prefix + tokenPrefix + name
}

func (r *RedisLS) getByName(conn redis.Conn, name string) (*RedisLSNode, error) {
	res, err := conn.Do("HGETALL", r.byNameKey(name))
	if err != nil {
		return nil, err
	}
	vals, err := redis.StringMap(res, nil)
	if err != nil {
		return nil, err
	}
	if vals[nameKey] == "" {
		if vals[expiryKey] == "" {
			return nil, nil
		} else {
			vals[nameKey] = name
		}
	}

	duration, _ := strconv.ParseInt(vals[durationKey], 10, 64)
	expiry, _ := strconv.ParseInt(vals[expiryKey], 10, 64)
	refCount, _ := strconv.ParseInt(vals[refCountKey], 10, 32)

	ret := &RedisLSNode{
		name: vals[nameKey],
		details: webdav.LockDetails{
			Root:      vals[rootKey],
			Duration:  time.Duration(duration),
			OwnerXML:  vals[ownerXMLKey],
			ZeroDepth: vals[zeroDepthKey] == trueValue,
		},
		token:    vals[tokenKey],
		refCount: int(refCount),
		expiry:   time.Unix(expiry, 0),
		held:     vals[heldKey] == trueValue,
	}

	return ret, nil
}

func (r *RedisLS) getByToken(conn redis.Conn, token string) (*RedisLSNode, error) {
	tokenName, err := redis.String(conn.Do("GET", r.byTokenKey(token)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, nil
		}
		return nil, err
	}
	n, err := r.getByName(conn, tokenName)
	if err != nil {
		return nil, err
	}
	return n, nil
}

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

	names, err := redis.Strings(conn.Do("ZRANGEBYSCORE", r.prefix+expiryZSetKey, "-inf", "+inf"))
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

var lockTestDurations = []time.Duration{
	infiniteTimeout, // infiniteTimeout means to never expire.
	0,               // A zero duration means to expire immediately.
	100 * time.Hour, // A very large duration will not expire in these tests.
}

// lockTestNames are the names of a set of mutually compatible locks. For each
// name fragment:
//   - _ means no explicit lock.
//   - i means a infinite-depth lock,
//   - z means a zero-depth lock,
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

	return NewRedisLS(pool, prefix)
}

func TestRedisLSConfirm(t *testing.T) {
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

func TestRedisLSNonCanonicalRoot(t *testing.T) {
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

func TestRedisLSExpiry(t *testing.T) {
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
			collectExpiredNodesScript := redis.NewScript(0,
				GetParentPathFunc+
					RemoveFunc+
					CollectExpiredNodesFunc+
					`return collect_expired_nodes(ARGV[1], tonumber(ARGV[2]))`,
			)

			_, err := collectExpiredNodesScript.Do(conn, r.prefix, now.Unix())
			if err != nil {
				panic(err)
			}
			got := make([]string, 0, byTokenLen(r))
			for _, n := range byTokenAll(r) {
				got = append(got, fmt.Sprintf("%s.%d",
					n.details.Root, n.expiry.Sub(zTime)/time.Second))
			}
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

func TestRedisLS(t *testing.T) {
	now := time.Unix(0, 0)
	r := NewTestRedisLS()
	rng := rand.New(rand.NewSource(0))
	tokens := map[string]string{}
	nConfirm, nCreate, nRefresh, nUnlock := 0, 0, 0, 0
	const N = 100

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
	r.redisLog("consistent start")
	defer r.redisLog("consistent end")

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
