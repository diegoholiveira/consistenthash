package cache_test

import (
	"context"
	"fmt"
	"testing"

	cache "github.com/diegoholiveira/go-caching"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

func TestCacheLibrary(t *testing.T) {
	compose, err := tc.NewDockerCompose("docker-compose.yml")
	require.NoError(t, err, "NewDockerComposeAPI()")

	t.Cleanup(func() {
		require.NoError(t, compose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveImagesLocal), "compose.Down()")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, compose.Up(ctx, tc.Wait(true)), "compose.Up()")

	addresses := make([]string, 0)
	for _, serverName := range compose.Services() {
		server, _ := compose.ServiceContainer(context.Background(), serverName)
		host, _ := server.Host(context.Background())
		port, _ := server.MappedPort(context.Background(), "6379/tcp")
		addresses = append(addresses, fmt.Sprintf("%s:%s", host, port.Port()))
	}

	t.Run("TestCacheLibrary", func(t *testing.T) {
		cache := cache.NewCache(
			cache.WithServers(addresses...),
			cache.WithNodeFinder(cache.NewConsistentHash(3)),
		)

		values := make(map[string]string)

		for i := 1; i <= 100; i++ {
			key := fmt.Sprintf("my-key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			values[key] = value
		}

		for key, value := range values {
			err := cache.Set(ctx, key, value)
			assert.NoError(t, err, "cache.Set()")
		}

		for key, value := range values {
			v, err := cache.Get(ctx, key)
			assert.NoError(t, err, "cache.Get()")
			assert.Equal(t, value, v, "cache.Get()")
		}
	})
}
