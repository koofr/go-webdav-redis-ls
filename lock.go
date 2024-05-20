// Copyright 2014 The Go Authors. All rights reserved.
// Copyright 2016 Koofr. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webdavredisls

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
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

	errLocked             = "ERR_LOCKED"
	errNoSuchLock         = "ERR_NO_SUCH_LOCK"
	errConfirmationFailed = "ERR_CONFIRMATION_FAILED"

	infiniteTimeout time.Duration = -1
)

func durationToSec(d time.Duration) int64 {
	if d == infiniteTimeout {
		return -1
	}
	return int64(d / time.Second)
}

type RedisLS struct {
	pool   *redis.Pool
	prefix string
}

// NewRedisLS returns a new Redis LockSystem.
func NewRedisLS(pool *redis.Pool, prefix string) *RedisLS {
	r := &RedisLS{
		pool:   pool,
		prefix: prefix,
	}

	return r
}

func (r *RedisLS) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (func(), error) {
	conn := r.pool.Get()
	defer conn.Close()

	if name0 != "" {
		name0 = slashClean(name0)
	}
	if name1 != "" {
		name1 = slashClean(name1)
	}

	conditionsLen := len(conditions)

	args := make([]interface{}, 5+conditionsLen)
	args[0] = r.prefix
	args[1] = now.Unix()
	args[2] = name0
	args[3] = name1
	args[4] = conditionsLen

	for i, condition := range conditions {
		// TODO: support Condition.Not and Condition.ETag.
		args[5+i] = condition.Token
	}

	res, err := ConfirmScript.Do(conn, args...)
	if err != nil {
		return nil, err
	}
	if reply, ok := res.([]byte); ok {
		replyStr := string(reply)
		if replyStr == errConfirmationFailed {
			return nil, webdav.ErrConfirmationFailed
		}
		return nil, fmt.Errorf("confirm error: %s", replyStr)
	}

	conditionNames, err := redis.Strings(res, nil)
	if err != nil {
		return nil, err
	}
	conditionName0 := conditionNames[0]
	conditionName1 := conditionNames[1]

	return func() {
		if conditionName0 != "" || conditionName1 != "" {
			conn := r.pool.Get()
			defer conn.Close()

			_, err = ReleaseScript.Do(
				conn,
				r.prefix,
				conditionName0,
				conditionName1,
			)
			if err != nil {
				// TODO we should not just ignore the error
				panic(err)
			}
		}
	}, nil
}

func (r *RedisLS) Create(now time.Time, details webdav.LockDetails) (string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	tokenOrErr, err := redis.String(CreateScript.Do(
		conn,
		r.prefix,
		now.Unix(),
		slashClean(details.Root),
		durationToSec(details.Duration),
		details.ZeroDepth,
		details.OwnerXML,
	))
	if err != nil {
		return "", err
	}
	if tokenOrErr == errLocked {
		return "", webdav.ErrLocked
	}

	return tokenOrErr, nil
}

func (r *RedisLS) Refresh(now time.Time, token string, duration time.Duration) (webdav.LockDetails, error) {
	conn := r.pool.Get()
	defer conn.Close()

	res, err := RefreshScript.Do(
		conn,
		r.prefix,
		now.Unix(),
		token,
		durationToSec(duration),
	)
	if err != nil {
		return webdav.LockDetails{}, err
	}
	if reply, ok := res.([]byte); ok {
		replyStr := string(reply)
		if replyStr == errLocked {
			return webdav.LockDetails{}, webdav.ErrLocked
		}
		if replyStr == errNoSuchLock {
			return webdav.LockDetails{}, webdav.ErrNoSuchLock
		}
		return webdav.LockDetails{}, fmt.Errorf("refresh error: %s", replyStr)
	}

	details, err := redis.StringMap(res, nil)
	if err != nil {
		return webdav.LockDetails{}, err
	}

	lockDetails := webdav.LockDetails{}
	lockDetails.Root = details[rootKey]
	lockDetailsDurationSec, _ := strconv.ParseInt(details[durationKey], 10, 64)
	lockDetails.Duration = time.Duration(lockDetailsDurationSec) * time.Second
	lockDetails.OwnerXML = details[ownerXMLKey]
	lockDetails.ZeroDepth = details[zeroDepthKey] == trueValue

	return lockDetails, nil
}

func (r *RedisLS) Unlock(now time.Time, token string) error {
	conn := r.pool.Get()
	defer conn.Close()

	res, err := UnlockScript.Do(
		conn,
		r.prefix,
		now.Unix(),
		token,
	)
	if err != nil {
		return err
	}
	if reply, ok := res.([]byte); ok {
		replyStr := string(reply)
		if replyStr == errLocked {
			return webdav.ErrLocked
		}
		if replyStr == errNoSuchLock {
			return webdav.ErrNoSuchLock
		}
		return fmt.Errorf("unlock error: %s", replyStr)
	}

	return nil
}

// slashClean is equivalent to but slightly more efficient than
// path.Clean("/" + name).
func slashClean(name string) string {
	if name == "" || name[0] != '/' {
		name = "/" + name
	}
	return path.Clean(name)
}
