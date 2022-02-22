package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/example/common"
)

var (
	ctx = context.Background()
)

func main() {
	//ExampleNewClient()
	ExampleClientOp()
}

func ExampleNewClient() {
	client := redis.NewClient(&redis.Options{
		Addr:     "herrypc:4025",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
}

func ExampleClientOp() {

	//client := redis.NewClient(&redis.Options{
	//	Addr:     "XX.XX.XX.XX:4025",
	//	Password: "", // no password set
	//	DB:       0,  // use default DB
	//})

	client := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName:            "TestDBARedis001_001",
		SentinelAddrs:         []string{common.SentinelAddr},
		SentinelPassword:      "",
		RouteByLatency:        false,
		Rws:                   true,
		RouteRandomly:         true,
		SlaveOnly:             true,
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

	//err := client.MGet(ctx, "key", "value4", "value3", "value3").Err()
	//if err != nil {
	//	panic(err)
	//}

	//err := client.Set(ctx, "key", "value4", 0).Err()
	//if err != nil {
	//	panic(err)
	//}
	//
	val, err := client.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	cursor := uint64(0)
	var keys []string
	keys, cursor, err = client.Scan(ctx, cursor, "", 2).Result()
	fmt.Printf("scan keys keys: %v, cursor: %d\n", keys, cursor)

	for cursor != 0 {
		keys, cursor, err = client.Scan(ctx, cursor, "", 2).Result()
		fmt.Printf("scan keys keys: %v, cursor: %d\n", keys, cursor)
	}

	//
	//val2, err := client.Get(ctx, "key2").Result()
	//if err == redis.Nil {
	//	fmt.Println("key2 does not exist")
	//} else if err != nil {
	//	panic(err)
	//} else {
	//	fmt.Println("key2", val2)
	//}

	//time.Sleep(1000 * time.Second)
	// Output: key value
	// key2 does not exist
}
