package webdavredisls_test

import (
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
	. "github.com/koofr/go-webdav-redis-ls"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LockLua", func() {
	var pool *redis.Pool
	var conn redis.Conn
	prefix := "webdavredislstest:"

	parentPathScript := redis.NewScript(0,
		GetParentPathFunc+
			`return get_parent_path(ARGV[1])`,
	)

	createTokenScript := redis.NewScript(0,
		GetParentPathFunc+
			CreateTokenFunc+
			`return create_token(ARGV[1], tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4]), ARGV[5] == "1", ARGV[6])`,
	)

	canCreateScript := redis.NewScript(0,
		GetParentPathFunc+
			CanCreateFunc+
			`return tostring(can_create(ARGV[1], ARGV[2], ARGV[3] == "1"))`,
	)

	removeScript := redis.NewScript(0,
		GetParentPathFunc+
			RemoveFunc+
			`return remove(ARGV[1], ARGV[2], ARGV[3], ARGV[4], tonumber(ARGV[5]))`,
	)

	collectExpiredNodesScript := redis.NewScript(0,
		GetParentPathFunc+
			RemoveFunc+
			CollectExpiredNodesFunc+
			`return collect_expired_nodes(ARGV[1], tonumber(ARGV[2]))`,
	)

	holdScript := redis.NewScript(0,
		HoldFunc+
			`return hold(ARGV[1], ARGV[2], tonumber(ARGV[3]))`,
	)

	unholdScript := redis.NewScript(0,
		UnholdFunc+
			`return unhold(ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]))`,
	)

	lookupScript := redis.NewScript(0,
		LookupFunc+
			`
			local condition_tokens_count = tonumber(ARGV[3])
			local condition_tokens = {unpack(ARGV, 4, 4 + condition_tokens_count)}
			local res = lookup(ARGV[1], ARGV[2], condition_tokens)
			if res ~= nil then
				res = {res[1], tostring(res[2])}
			end
			return res
			`,
	)

	BeforeEach(func() {
		server := os.Getenv("REDIS_SERVER")
		if server == "" {
			server = "localhost:6379"
		}

		pool = &redis.Pool{
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

		conn = pool.Get()

		keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
		Expect(err).NotTo(HaveOccurred())

		for _, key := range keys {
			_, err = conn.Do("DEL", key)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		conn.Close()
	})

	Describe("GetParentPathFunc", func() {
		It("should get parent path", func() {
			parentPath, err := redis.String(parentPathScript.Do(conn, "/path"))
			Expect(err).NotTo(HaveOccurred())
			Expect(parentPath).To(Equal("/"))

			parentPath, err = redis.String(parentPathScript.Do(conn, "/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(parentPath).To(Equal("/"))

			parentPath, err = redis.String(parentPathScript.Do(conn, "/path/to"))
			Expect(err).NotTo(HaveOccurred())
			Expect(parentPath).To(Equal("/path"))
		})
	})

	Describe("CreateTokenFunc", func() {
		It("should create a token with a non-negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			nt, err := redis.Int64(conn.Do("GET", prefix+"nt"))
			Expect(err).NotTo(HaveOccurred())
			Expect(nt).To(Equal(int64(1)))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			path, err := redis.String(conn.Do("GET", prefix+"t:1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2"))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(BeEmpty())

			paths, err = redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+durationSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
			}))
		})

		It("should create a token with a negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := -1
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",    // name
				"r": "/p1/p2",    // root
				"h": "f",         // held
				"t": "1",         // token
				"d": "-1",        // duration
				"o": "<owner />", // ownerXML
				"z": "t",         // isZeroDepth
				"e": "0",         // expiry
				"c": "1",         // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			path, err := redis.String(conn.Do("GET", prefix+"t:1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2"))
		})

		It("should create a non-zero-depth token", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := false
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "f",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))
		})
	})

	Describe("CanCreateTokenFunc", func() {
		It("should return false for a non-existent path", func() {
			root := "/p1/p2"
			isZeroDepth := true

			canCreate, err := redis.Bool(canCreateScript.Do(
				conn,
				prefix,
				root,
				isZeroDepth,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(canCreate).To(BeTrue())
		})

		DescribeTable("CanCreateTokenFunc table",
			func(rootPath string, rootIsZeroDepth bool, canCreateName string, canCreateIsZeroDepth bool, expected bool) {
				_, err := createTokenScript.Do(
					conn,
					prefix,
					1556895905,
					rootPath,
					300,
					rootIsZeroDepth,
					"",
				)
				Expect(err).NotTo(HaveOccurred())

				canCreate, err := redis.Bool(canCreateScript.Do(
					conn,
					prefix,
					canCreateName,
					canCreateIsZeroDepth,
				))
				Expect(err).NotTo(HaveOccurred())
				Expect(canCreate).To(Equal(expected))
			},
			Entry("zero-depth: same path (can create zero-depth)", "/p1/p2", true, "/p1/p2", true, false),
			Entry("zero-depth: same path (can create non-zero-depth)", "/p1/p2", true, "/p1/p2", false, false),
			Entry("zero-depth: parent path (can create zero-depth)", "/p1/p2", true, "/p1", true, true),
			Entry("zero-depth: parent path (can create non-zero-depth)", "/p1/p2", true, "/p1", false, false),
			Entry("zero-depth: child path (can create zero-depth)", "/p1/p2", true, "/p1/p2/p3", true, true),
			Entry("zero-depth: child path (can create non-zero-depth)", "/p1/p2", true, "/p1/p2/p3", false, true),
			Entry("non-zero-depth: same path (can create zero-depth)", "/p1/p2", false, "/p1/p2", true, false),
			Entry("non-zero-depth: same path (can create non-zero-depth)", "/p1/p2", false, "/p1/p2", false, false),
			Entry("non-zero-depth: parent path (can create zero-depth)", "/p1/p2", false, "/p1", true, true),
			Entry("non-zero-depth: parent path (can create non-zero-depth)", "/p1/p2", false, "/p1", false, false),
			Entry("non-zero-depth: child path (can create zero-depth)", "/p1/p2", false, "/p1/p2/p3", true, false),
			Entry("non-zero-depth: child path (can create non-zero-depth)", "/p1/p2", false, "/p1/p2/p3", false, false),
		)
	})

	Describe("RemoveFunc", func() {
		It("should remove a node with a non-negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = removeScript.Do(
				conn,
				prefix,
				root,
				root,
				token,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix + "nt",
			))
		})

		It("should remove a node with a negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := -1
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = removeScript.Do(
				conn,
				prefix,
				root,
				root,
				token,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix + "nt",
			))
		})

		It("should remove a child node and then the parent", func() {
			nowSec1 := 1556895905
			root1 := "/p1/p2"
			durationSec1 := 300
			isZeroDepth1 := true
			ownerXML1 := "<owner />"

			token1, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec1,
				root1,
				durationSec1,
				isZeroDepth1,
				ownerXML1,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token1).To(Equal("1"))

			nowSec2 := 1556895906
			root2 := "/p1/p2/p3"
			durationSec2 := 300
			isZeroDepth2 := true
			ownerXML2 := "<owner />"

			token2, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec2,
				root2,
				durationSec2,
				isZeroDepth2,
				ownerXML2,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token2).To(Equal("2"))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"n:/p1/p2/p3",
				prefix+"t:1",
				prefix+"t:2",
				prefix+"e",
			))

			nt, err := redis.Int64(conn.Do("GET", prefix+"nt"))
			Expect(err).NotTo(HaveOccurred())
			Expect(nt).To(Equal(int64(2)))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "2",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "2",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "2", // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2/p3"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2/p3",  // name
				"r": "/p1/p2/p3",  // root
				"h": "f",          // held
				"t": "2",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896206", // expiry
				"c": "1",          // refCount
			}))

			path, err := redis.String(conn.Do("GET", prefix+"t:1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2"))

			path, err = redis.String(conn.Do("GET", prefix+"t:2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2/p3"))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec2+durationSec2+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
				"/p1/p2/p3",
			}))

			_, err = removeScript.Do(
				conn,
				prefix,
				root2,
				root2,
				token2,
				durationSec2,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err = redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			nt, err = redis.Int64(conn.Do("GET", prefix+"nt"))
			Expect(err).NotTo(HaveOccurred())
			Expect(nt).To(Equal(int64(2)))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			path, err = redis.String(conn.Do("GET", prefix+"t:1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2"))

			paths, err = redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec2+durationSec2+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
			}))

			_, err = removeScript.Do(
				conn,
				prefix,
				root1,
				root1,
				token1,
				durationSec1,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err = redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix + "nt",
			))
		})

		It("should remove a parent node and then the child", func() {
			nowSec1 := 1556895905
			root1 := "/p1/p2"
			durationSec1 := 300
			isZeroDepth1 := true
			ownerXML1 := "<owner />"

			token1, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec1,
				root1,
				durationSec1,
				isZeroDepth1,
				ownerXML1,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token1).To(Equal("1"))

			nowSec2 := 1556895906
			root2 := "/p1/p2/p3"
			durationSec2 := 300
			isZeroDepth2 := true
			ownerXML2 := "<owner />"

			token2, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec2,
				root2,
				durationSec2,
				isZeroDepth2,
				ownerXML2,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token2).To(Equal("2"))

			_, err = removeScript.Do(
				conn,
				prefix,
				root1,
				root1,
				token1,
				durationSec1,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"n:/p1/p2/p3",
				prefix+"t:2",
				prefix+"e",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2", // name
				"r": "/p1/p2", // root
				"h": "f",      // held
				// no token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			path, err := redis.String(conn.Do("GET", prefix+"t:2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2/p3"))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec2+durationSec2+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2/p3",
			}))

			_, err = removeScript.Do(
				conn,
				prefix,
				root2,
				root2,
				token2,
				durationSec2,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err = redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix + "nt",
			))
		})
	})

	Describe("CollectExpiredNodesFunc", func() {
		It("should not remove anything", func() {
			_, err := collectExpiredNodesScript.Do(
				conn,
				prefix,
				1556895905,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(BeEmpty())
		})

		It("should remove expired nodes", func() {
			nowSec1 := 1556895905
			root1 := "/p1/p2"
			durationSec1 := 310
			isZeroDepth1 := true
			ownerXML1 := "<owner />"

			token1, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec1,
				root1,
				durationSec1,
				isZeroDepth1,
				ownerXML1,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token1).To(Equal("1"))

			nowSec2 := 1556895906
			root2 := "/p1/p2/p3"
			durationSec2 := 300
			isZeroDepth2 := true
			ownerXML2 := "<owner />"

			token2, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec2,
				root2,
				durationSec2,
				isZeroDepth2,
				ownerXML2,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token2).To(Equal("2"))

			_, err = collectExpiredNodesScript.Do(
				conn,
				prefix,
				1556896207,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			_, err = collectExpiredNodesScript.Do(
				conn,
				prefix,
				1556896217,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err = redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix + "nt",
			))
		})

		It("should not remove nodes with negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := -1
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = collectExpiredNodesScript.Do(
				conn,
				prefix,
				1556896206,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",    // name
				"r": "/p1/p2",    // root
				"h": "f",         // held
				"t": "1",         // token
				"d": "-1",        // duration
				"o": "<owner />", // ownerXML
				"z": "t",         // isZeroDepth
				"e": "0",         // expiry
				"c": "1",         // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			path, err := redis.String(conn.Do("GET", prefix+"t:1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(Equal("/p1/p2"))
		})
	})

	Describe("HoldFunc", func() {
		It("should hold a node with a non-negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "t",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))
		})

		It("should hold a node with a negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := -1
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",    // name
				"r": "/p1/p2",    // root
				"h": "t",         // held
				"t": "1",         // token
				"d": "-1",        // duration
				"o": "<owner />", // ownerXML
				"z": "t",         // isZeroDepth
				"e": "0",         // expiry
				"c": "1",         // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))
		})

		It("should not hold a node twice", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("inconsistent held state"))
		})
	})

	Describe("UnholdFunc", func() {
		It("should unhold a node with a non-negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = unholdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
				nowSec+durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896205", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+durationSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
			}))
		})

		It("should unhold a node with a negative duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := -1
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = unholdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
				0,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",    // name
				"r": "/p1/p2",    // root
				"h": "f",         // held
				"t": "1",         // token
				"d": "-1",        // duration
				"o": "<owner />", // ownerXML
				"z": "t",         // isZeroDepth
				"e": "0",         // expiry
				"c": "1",         // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))
		})

		It("should not hold an unhold node", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(createTokenScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("1"))

			_, err = unholdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
				nowSec+durationSec,
			)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("inconsistent held state"))
		})
	})

	Describe("LookupFunc", func() {
		It("should find a node", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := redis.Strings(lookupScript.Do(
				conn,
				prefix,
				"/p1/p2",
				2,
				"9999",
				token,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]string{"/p1/p2", "300"}))
		})

		It("should not find a node", func() {
			res, err := lookupScript.Do(
				conn,
				prefix,
				"/p1/p2",
				2,
				"1",
				"2",
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeNil())
		})

		It("should find a held node", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			res, err := lookupScript.Do(
				conn,
				prefix,
				"/p1/p2",
				1,
				token,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeNil())
		})

		It("should find a parent node (non-zero-depth)", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := false
			ownerXML := "<owner />"

			token, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := redis.Strings(lookupScript.Do(
				conn,
				prefix,
				"/p1/p2/p3",
				1,
				token,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]string{"/p1/p2", "300"}))
		})

		It("should find a root node (non-zero-depth)", func() {
			nowSec := 1556895905
			root := "/"
			durationSec := 300
			isZeroDepth := false
			ownerXML := "<owner />"

			token, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := redis.Strings(lookupScript.Do(
				conn,
				prefix,
				"/p1",
				1,
				token,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]string{"/", "300"}))
		})

		It("should not find a parent node (zero-depth)", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := lookupScript.Do(
				conn,
				prefix,
				"/p1/p2/p3",
				1,
				token,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeNil())
		})
	})

	Describe("Create", func() {
		It("should create a token", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
		})

		It("should not create a token if it already exists", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))

			tokenOrErr, err = redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("ERR_LOCKED"))
		})

		It("should collect expired nodes", func() {
			nowSec1 := 1556895905
			root1 := "/p1/p2"
			durationSec1 := 300
			isZeroDepth1 := true
			ownerXML1 := "<owner />"

			tokenOrErr1, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec1,
				root1,
				durationSec1,
				isZeroDepth1,
				ownerXML1,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr1).To(Equal("1"))

			nowSec2 := 1556896905
			root2 := "/p1/p2"
			durationSec2 := 300
			isZeroDepth2 := true
			ownerXML2 := "<owner />"

			tokenOrErr2, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec2,
				root2,
				durationSec2,
				isZeroDepth2,
				ownerXML2,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr2).To(Equal("2"))

			ok, err := redis.Bool(conn.Do("EXISTS", prefix+"t:1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})

	Describe("Refresh", func() {
		It("should extend the duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			nowSec = 1556895907
			newDurationSec := 600

			details, err := redis.StringMap(RefreshScript.Do(
				conn,
				prefix,
				nowSec,
				token,
				newDurationSec,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(details).To(Equal(map[string]string{
				"r": "/p1/p2",    // root
				"d": "600",       // duration
				"o": "<owner />", // owner
				"z": "t",         // isZeroDepth
			}))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "600",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896507", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+durationSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(BeEmpty())

			paths, err = redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+newDurationSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
			}))
		})

		It("should shorten the duration", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 600
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			nowSec = 1556895907
			newDurationSec := 300

			details, err := redis.StringMap(RefreshScript.Do(
				conn,
				prefix,
				nowSec,
				token,
				newDurationSec,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(details).To(Equal(map[string]string{
				"r": "/p1/p2",    // root
				"d": "300",       // duration
				"o": "<owner />", // owner
				"z": "t",         // isZeroDepth
			}))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896207", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+newDurationSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
			}))
		})

		It("should set duration to non-negative", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := -1
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			nowSec = 1556895907
			newDurationSec := 300

			details, err := redis.StringMap(RefreshScript.Do(
				conn,
				prefix,
				nowSec,
				token,
				newDurationSec,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(details).To(Equal(map[string]string{
				"r": "/p1/p2",    // root
				"d": "300",       // duration
				"o": "<owner />", // owner
				"z": "t",         // isZeroDepth
			}))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
				prefix+"e",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",     // name
				"r": "/p1/p2",     // root
				"h": "f",          // held
				"t": "1",          // token
				"d": "300",        // duration
				"o": "<owner />",  // ownerXML
				"z": "t",          // isZeroDepth
				"e": "1556896207", // expiry
				"c": "1",          // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))

			paths, err := redis.Strings(conn.Do("ZRANGEBYSCORE", prefix+"e", "-inf", nowSec+newDurationSec+1))
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(Equal([]string{
				"/p1/p2",
			}))
		})

		It("should set duration to negative", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			nowSec = 1556895907
			newDurationSec := -1

			details, err := redis.StringMap(RefreshScript.Do(
				conn,
				prefix,
				nowSec,
				token,
				newDurationSec,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(details).To(Equal(map[string]string{
				"r": "/p1/p2",    // root
				"d": "-1",        // duration
				"o": "<owner />", // owner
				"z": "t",         // isZeroDepth
			}))

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix+"nt",
				prefix+"n:/p1/p2",
				prefix+"n:/p1",
				prefix+"n:/",
				prefix+"t:1",
			))

			m, err := redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1/p2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1/p2",    // name
				"r": "/p1/p2",    // root
				"h": "f",         // held
				"t": "1",         // token
				"d": "-1",        // duration
				"o": "<owner />", // ownerXML
				"z": "t",         // isZeroDepth
				"e": "0",         // expiry
				"c": "1",         // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/p1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/p1", // name
				"r": "/p1", // root
				"h": "f",   // held
				"c": "1",   // refCount
			}))

			m, err = redis.StringMap(conn.Do("HGETALL", prefix+"n:/"))
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{
				"n": "/", // name
				"r": "/", // root
				"h": "f", // held
				"c": "1", // refCount
			}))
		})

		It("should fail if lock does not exist", func() {
			nowSec := 1556895907
			token := 1
			newDurationSec := -1

			webdavErr, err := redis.String(RefreshScript.Do(
				conn,
				prefix,
				nowSec,
				token,
				newDurationSec,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(webdavErr).To(Equal("ERR_NO_SUCH_LOCK"))
		})

		It("should fail if lock is held", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			nowSec = 1556895907
			newDurationSec := 600

			webdavErr, err := redis.String(RefreshScript.Do(
				conn,
				prefix,
				nowSec,
				token,
				newDurationSec,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(webdavErr).To(Equal("ERR_LOCKED"))
		})
	})

	Describe("Unlock", func() {
		It("should unlock the node", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			nowSec = 1556895907

			_, err = UnlockScript.Do(
				conn,
				prefix,
				nowSec,
				token,
			)
			Expect(err).NotTo(HaveOccurred())

			keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf(
				prefix + "nt",
			))
		})

		It("should fail if lock does not exist", func() {
			nowSec := 1556895907
			token := 1

			webdavErr, err := redis.String(UnlockScript.Do(
				conn,
				prefix,
				nowSec,
				token,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(webdavErr).To(Equal("ERR_NO_SUCH_LOCK"))
		})

		It("should fail if lock is held", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			tokenOrErr, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(tokenOrErr).To(Equal("1"))
			token := tokenOrErr

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			webdavErr, err := redis.String(UnlockScript.Do(
				conn,
				prefix,
				nowSec,
				token,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(webdavErr).To(Equal("ERR_LOCKED"))
		})
	})

	Describe("Confirm", func() {
		It("should hold the nodes", func() {
			nowSec1 := 1556895905
			root1 := "/p1/p2"
			durationSec1 := 300
			isZeroDepth1 := true
			ownerXML1 := "<owner />"

			token1, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec1,
				root1,
				durationSec1,
				isZeroDepth1,
				ownerXML1,
			))
			Expect(err).NotTo(HaveOccurred())

			nowSec2 := 1556895905
			root2 := "/p4"
			durationSec2 := 300
			isZeroDepth2 := true
			ownerXML2 := "<owner />"

			token2, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec2,
				root2,
				durationSec2,
				isZeroDepth2,
				ownerXML2,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := redis.Strings(ConfirmScript.Do(
				conn,
				prefix,
				nowSec2,
				"/p1/p2",
				"/p4",
				2,
				token2,
				token1,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]string{"/p1/p2", "/p4"}))
		})

		It("should hold a single node", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			token, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := redis.Strings(ConfirmScript.Do(
				conn,
				prefix,
				nowSec,
				"/p1/p2",
				"",
				1,
				token,
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]string{"/p1/p2", ""}))
		})

		It("should fail for non-existent token", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			_, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			res, err := redis.String(ConfirmScript.Do(
				conn,
				prefix,
				nowSec,
				"/p1/p2",
				"",
				1,
				"9999",
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("ERR_CONFIRMATION_FAILED"))
		})
	})

	Describe("Release", func() {
		It("should release the nodes", func() {
			nowSec1 := 1556895905
			root1 := "/p1/p2"
			durationSec1 := 300
			isZeroDepth1 := true
			ownerXML1 := "<owner />"

			_, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec1,
				root1,
				durationSec1,
				isZeroDepth1,
				ownerXML1,
			))
			Expect(err).NotTo(HaveOccurred())

			nowSec2 := 1556895905
			root2 := "/p1/p2/p3"
			durationSec2 := 300
			isZeroDepth2 := true
			ownerXML2 := "<owner />"

			_, err = redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec2,
				root2,
				durationSec2,
				isZeroDepth2,
				ownerXML2,
			))
			Expect(err).NotTo(HaveOccurred())

			_, err = holdScript.Do(
				conn,
				prefix,
				root1,
				durationSec1,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = holdScript.Do(
				conn,
				prefix,
				root2,
				durationSec2,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = ReleaseScript.Do(
				conn,
				prefix,
				root1,
				root2,
			)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should release a single node", func() {
			nowSec := 1556895905
			root := "/p1/p2"
			durationSec := 300
			isZeroDepth := true
			ownerXML := "<owner />"

			_, err := redis.String(CreateScript.Do(
				conn,
				prefix,
				nowSec,
				root,
				durationSec,
				isZeroDepth,
				ownerXML,
			))
			Expect(err).NotTo(HaveOccurred())

			_, err = holdScript.Do(
				conn,
				prefix,
				root,
				durationSec,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = ReleaseScript.Do(
				conn,
				prefix,
				root,
				nil,
			)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
