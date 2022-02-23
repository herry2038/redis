package common

import (
	"github.com/go-redis/redis/v8"
)

var (
	SentinelAddr = "XXXX:20019"
	DirectAddr   = "herrypc:4025"
)

func GetDirectClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     DirectAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return client
}

func GetMasterClient() *redis.Client {
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:            "TestDBARedis001_001",
		SentinelAddrs:         []string{SentinelAddr},
		SentinelPassword:      "",
		RouteByLatency:        false,
		RouteRandomly:         false,
		SlaveOnly:             false,
		UseDisconnectedSlaves: false,
		Dialer:                nil,
		OnConnect:             nil,
		Username:              "",
		Password:              "",
		DB:                    0,
		MaxRetries:            0,
		MinRetryBackoff:       0,
		MaxRetryBackoff:       0,
		DialTimeout:           0,
		ReadTimeout:           0,
		WriteTimeout:          0,
		PoolFIFO:              false,
		PoolSize:              0,
		MinIdleConns:          0,
		MaxConnAge:            0,
		PoolTimeout:           0,
		IdleTimeout:           0,
		IdleCheckFrequency:    0,
		TLSConfig:             nil,
	})
	return client
}
