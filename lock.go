// Copyright 2014 The Go Authors. All rights reserved.
// Copyright 2016 Koofr. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webdavredisls

import (
	"errors"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	webdav "github.com/koofr/go-webdav"
)

const (
	namePrefix  string = "n:"
	tokenPrefix string = "t:"

	expiryZSetKey string = "e"
	nextTokenKey  string = "nt"

	nameKey      string = "n"
	rootKey      string = "r"
	durationKey  string = "d"
	ownerXMLKey  string = "o"
	zeroDepthKey string = "z"
	tokenKey     string = "t"
	refCountKey  string = "c"
	expiryKey    string = "e"
	heldKey      string = "h"

	trueValue  string = "t"
	falseValue string = "f"

	infiniteTimeout time.Duration = -1
)

type RedisLS struct {
	pool          *redis.Pool
	prefix        string
	expiryZSetKey string
	nextTokenKey  string
	mu            Mutex
}

// NewRedisLS returns a new Redis LockSystem.
func NewRedisLS(pool *redis.Pool, prefix string, mutex Mutex) *RedisLS {
	r := &RedisLS{
		pool:          pool,
		prefix:        prefix,
		expiryZSetKey: prefix + expiryZSetKey,
		nextTokenKey:  prefix + nextTokenKey,
		mu:            mutex,
	}

	return r
}

func (r *RedisLS) byNameKey(name string) string {
	return r.prefix + namePrefix + name
}

func (r *RedisLS) byTokenKey(name string) string {
	return r.prefix + tokenPrefix + name
}

func (r *RedisLS) connDo(conn redis.Conn, commandName string, args ...interface{}) (interface{}, error) {
	return conn.Do(commandName, args...)
}

func (r *RedisLS) nextToken(conn redis.Conn) (string, error) {
	token, err := redis.Int64(r.connDo(conn, "INCR", r.nextTokenKey))
	if err != nil {
		return "", err
	}

	return strconv.FormatInt(token, 10), nil
}

func (r *RedisLS) getByName(conn redis.Conn, name string) (*RedisLSNode, error) {
	res, err := r.connDo(conn, "HGETALL", r.byNameKey(name))
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
	tokenName, err := redis.String(r.connDo(conn, "GET", r.byTokenKey(token)))
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

func (r *RedisLS) collectExpiredNodes(conn redis.Conn, now time.Time) error {
	for {
		names, err := redis.Strings(r.connDo(conn, "ZRANGEBYSCORE", r.expiryZSetKey, "-inf", strconv.FormatInt(now.Unix(), 10), "LIMIT", 0, 100))
		if err != nil {
			return err
		}
		if len(names) == 0 {
			break
		}

		for _, name := range names {
			n, err := r.getByName(conn, name)
			if err != nil {
				return err
			}
			if n != nil {
				err = r.remove(conn, n)
				if err != nil {
					return err
				}
			} else {
				// inconsistent state. what do we do now?
			}
		}
	}

	return nil
}

func (r *RedisLS) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (func(), error) {
	err := r.mu.Lock()
	if err != nil {
		return nil, err
	}
	defer r.mu.Unlock()
	conn := r.pool.Get()
	defer conn.Close()

	err = r.collectExpiredNodes(conn, now)
	if err != nil {
		return nil, err
	}

	var n0, n1 *RedisLSNode

	if name0 != "" {
		n0, err = r.lookup(conn, slashClean(name0), conditions...)
		if err != nil {
			return nil, err
		}
		if n0 == nil {
			return nil, webdav.ErrConfirmationFailed
		}
	}
	if name1 != "" {
		n1, err = r.lookup(conn, slashClean(name1), conditions...)
		if err != nil {
			return nil, err
		}
		if n1 == nil {
			return nil, webdav.ErrConfirmationFailed
		}
	}

	// Don't hold the same node twice.
	if n1 != nil && n1.name == n0.name {
		n1 = nil
	}

	if n0 != nil {
		r.hold(conn, n0)
	}
	if n1 != nil {
		r.hold(conn, n1)
	}

	return func() {
		err := r.mu.Lock()
		if err != nil {
			// TODO we should not just ignore the error
		}
		defer r.mu.Unlock()

		if n0 != nil || n1 != nil {
			conn := r.pool.Get()
			defer conn.Close()
			if n1 != nil {
				err := r.unhold(conn, n1)
				if err != nil {
					// TODO we should not just ignore the error
				}
			}
			if n0 != nil {
				err := r.unhold(conn, n0)
				if err != nil {
					// TODO we should not just ignore the error
				}
			}
		}
	}, nil
}

// lookup returns the node n that locks the named resource, provided that n
// matches at least one of the given conditions and that lock isn't held by
// another party. Otherwise, it returns nil.
//
// n may be a parent of the named resource, if n is an infinite depth lock.
func (r *RedisLS) lookup(conn redis.Conn, name string, conditions ...webdav.Condition) (n *RedisLSNode, err error) {
	// TODO: support Condition.Not and Condition.ETag.
	for _, c := range conditions {
		n, err = r.getByToken(conn, c.Token)
		if err != nil {
			return nil, err
		}
		if n == nil || n.held {
			continue
		}
		if name == n.details.Root {
			return n, nil
		}
		if n.details.ZeroDepth {
			continue
		}
		if n.details.Root == "/" || strings.HasPrefix(name, n.details.Root+"/") {
			return n, nil
		}
	}
	return nil, nil
}

func (r *RedisLS) hold(conn redis.Conn, n *RedisLSNode) error {
	heldStr, err := redis.String(r.connDo(conn, "HGET", r.byNameKey(n.name), heldKey))
	if err != nil {
		return err
	}
	if heldStr == trueValue {
		return errors.New("webdav: RedisLS inconsistent held state")
	}

	_, err = r.connDo(conn, "HMSET", r.byNameKey(n.name), heldKey, trueValue)
	if err != nil {
		return err
	}

	if n.details.Duration >= 0 {
		_, err = r.connDo(conn, "ZREM", r.expiryZSetKey, n.name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RedisLS) unhold(conn redis.Conn, n *RedisLSNode) error {
	heldStr, err := redis.String(r.connDo(conn, "HGET", r.byNameKey(n.name), heldKey))
	if err != nil {
		return err
	}
	if heldStr != trueValue {
		return errors.New("webdav: RedisLS inconsistent held state")
	}

	_, err = r.connDo(conn, "HMSET", r.byNameKey(n.name), heldKey, falseValue)
	if err != nil {
		return err
	}

	if n.details.Duration >= 0 {
		_, err = r.connDo(conn, "ZADD", r.expiryZSetKey, n.expiry.Unix(), n.name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RedisLS) Create(now time.Time, details webdav.LockDetails) (string, error) {
	err := r.mu.Lock()
	if err != nil {
		return "", err
	}
	defer r.mu.Unlock()
	conn := r.pool.Get()
	defer conn.Close()

	err = r.collectExpiredNodes(conn, now)
	if err != nil {
		return "", err
	}

	details.Root = slashClean(details.Root)

	ok, err := r.canCreate(conn, details.Root, details.ZeroDepth)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", webdav.ErrLocked
	}
	token, err := r.create(conn, now, details)
	if err != nil {
		return "", err
	}
	return token, nil
}

func (r *RedisLS) Refresh(now time.Time, token string, duration time.Duration) (webdav.LockDetails, error) {
	err := r.mu.Lock()
	if err != nil {
		return webdav.LockDetails{}, err
	}
	defer r.mu.Unlock()
	conn := r.pool.Get()
	defer conn.Close()

	err = r.collectExpiredNodes(conn, now)
	if err != nil {
		return webdav.LockDetails{}, err
	}

	n, err := r.getByToken(conn, token)
	if err != nil {
		return webdav.LockDetails{}, err
	}
	if n == nil {
		return webdav.LockDetails{}, webdav.ErrNoSuchLock
	}
	if n.held {
		return webdav.LockDetails{}, webdav.ErrLocked
	}
	if n.expiry.Unix() != 0 && duration < 0 {
		_, err = r.connDo(conn, "ZREM", r.expiryZSetKey, n.name)
		if err != nil {
			return webdav.LockDetails{}, err
		}
	}
	n.details.Duration = duration

	expiry := int64(0)

	if duration >= 0 {
		n.expiry = now.Add(duration)

		expiry = n.expiry.Unix()

		_, err = r.connDo(conn, "ZADD", r.expiryZSetKey, expiry, n.name)
		if err != nil {
			return webdav.LockDetails{}, err
		}
	}

	_, err = r.connDo(conn, "HMSET", r.byNameKey(n.name), durationKey, int64(duration), expiryKey, expiry)
	if err != nil {
		return webdav.LockDetails{}, err
	}

	return n.details, nil
}

func (r *RedisLS) Unlock(now time.Time, token string) error {
	err := r.mu.Lock()
	if err != nil {
		return err
	}
	defer r.mu.Unlock()
	conn := r.pool.Get()
	defer conn.Close()

	err = r.collectExpiredNodes(conn, now)
	if err != nil {
		return err
	}

	n, err := r.getByToken(conn, token)
	if err != nil {
		return err
	}
	if n == nil {
		return webdav.ErrNoSuchLock
	}
	if n.held {
		return webdav.ErrLocked
	}
	err = r.remove(conn, n)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisLS) canCreate(conn redis.Conn, name string, zeroDepth bool) (bool, error) {
	return walkToRoot(name, func(name0 string, first bool) (bool, error) {
		n, err := r.getByName(conn, name0)
		if err != nil {
			return false, err
		}
		if n == nil {
			return true, nil
		}
		if first {
			if n.token != "" {
				// The target node is already locked.
				return false, nil
			}
			if !zeroDepth {
				// The requested lock depth is infinite, and the fact that n exists
				// (n != nil) means that a descendent of the target node is locked.
				return false, nil
			}
		} else if n.token != "" && !n.details.ZeroDepth {
			// An ancestor of the target node is locked with infinite depth.
			return false, nil
		}
		return true, nil
	})
}

func (r *RedisLS) create(conn redis.Conn, now time.Time, details webdav.LockDetails) (token string, err error) {
	token, err = r.nextToken(conn)
	if err != nil {
		return "", err
	}

	_, err = walkToRoot(details.Root, func(name0 string, first bool) (bool, error) {
		refCount, err := redis.Int64(r.connDo(conn, "HINCRBY", r.byNameKey(name0), refCountKey, 1))
		if err != nil {
			return false, err
		}

		args := []interface{}{r.byNameKey(name0)}

		if refCount == 1 {
			args = append(
				args,
				nameKey, name0,
				rootKey, name0,
				refCountKey, 1,
				heldKey, falseValue)
		}

		expiry := int64(0)

		if first {
			if details.Duration >= 0 {
				expiry = now.Add(details.Duration).Unix()
			}

			zeroDepth := trueValue
			if !details.ZeroDepth {
				zeroDepth = falseValue
			}

			args = append(
				args,
				tokenKey, token,
				durationKey, int64(details.Duration),
				ownerXMLKey, details.OwnerXML,
				zeroDepthKey, zeroDepth,
				expiryKey, expiry)
		}

		if len(args) > 1 {
			_, err = r.connDo(conn, "HMSET", args...)
			if err != nil {
				return false, err
			}
		}

		if first {
			_, err := r.connDo(conn, "SET", r.byTokenKey(token), name0)
			if err != nil {
				return false, err
			}

			if details.Duration >= 0 {
				_, err = r.connDo(conn, "ZADD", r.expiryZSetKey, expiry, name0)
				if err != nil {
					return false, err
				}
			}
		}

		return true, nil
	})
	if err != nil {
		return "", err
	}

	return token, nil
}

func (r *RedisLS) remove(conn redis.Conn, n *RedisLSNode) error {
	_, err := r.connDo(conn, "DEL", r.byTokenKey(n.token))
	if err != nil {
		return err
	}

	_, err = r.connDo(conn, "HDEL", r.byNameKey(n.name), tokenKey)
	if err != nil {
		return err
	}

	_, err = walkToRoot(n.details.Root, func(name0 string, first bool) (bool, error) {
		refCount, err := redis.Int64(r.connDo(conn, "HINCRBY", r.byNameKey(name0), refCountKey, -1))
		if err != nil {
			return false, err
		}
		if refCount == 0 {
			_, err := r.connDo(conn, "DEL", r.byNameKey(name0))
			if err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	if n.details.Duration >= 0 {
		_, err = r.connDo(conn, "ZREM", r.expiryZSetKey, n.name)
		if err != nil {
			return err
		}
	}

	return nil
}

func walkToRoot(name string, f func(name0 string, first bool) (bool, error)) (bool, error) {
	for first := true; ; first = false {
		ok, err := f(name, first)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if name == "/" {
			break
		}
		name = name[:strings.LastIndex(name, "/")]
		if name == "" {
			name = "/"
		}
	}
	return true, nil
}

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

type Mutex interface {
	Lock() error
	Unlock() bool
}

// slashClean is equivalent to but slightly more efficient than
// path.Clean("/" + name).
func slashClean(name string) string {
	if name == "" || name[0] != '/' {
		name = "/" + name
	}
	return path.Clean(name)
}
