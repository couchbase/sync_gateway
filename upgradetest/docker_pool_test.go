package upgradetest

import (
	"log"
	"testing"

	"github.com/ory/dockertest/v3"
)

var globalDockertestPool *dockertest.Pool

func newDockertestPool(_ *testing.M) *dockertest.Pool {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	return pool
}
