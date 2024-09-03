package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type NodeFinder interface {
	AddNode(node string)
	RemoveNode(node string)
	FindNodeByCacheKey(key string) string
}

type Cache struct {
	servers   []string
	resolver  NodeFinder
	instances map[string]*redis.Client
}

type Option func(*Cache)

func WithServers(addresses ...string) Option {
	return func(c *Cache) {
		c.servers = addresses
	}
}

func WithNodeFinder(finder NodeFinder) Option {
	return func(c *Cache) {
		c.resolver = finder
	}
}

func NewCache(options ...func(*Cache)) *Cache {
	cache := &Cache{
		instances: make(map[string]*redis.Client),
	}

	for _, option := range options {
		option(cache)
	}

	cache.init()

	return cache
}

func (c *Cache) init() {
	for _, server := range c.servers {
		c.resolver.AddNode(server)

		client := redis.NewClient(&redis.Options{
			Addr: server,
		})

		c.instances[server] = client
	}
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	node := c.resolver.FindNodeByCacheKey(key)
	client := c.instances[node]

	return client.Get(ctx, key).Result()
}

func (c *Cache) Set(ctx context.Context, key, value string) error {
	node := c.resolver.FindNodeByCacheKey(key)
	client := c.instances[node]

	return client.Set(ctx, key, value, 0).Err()
}
