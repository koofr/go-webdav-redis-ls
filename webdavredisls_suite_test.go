package webdavredisls_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGoWebdavRedisLs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoWebdavRedisLs Suite")
}
