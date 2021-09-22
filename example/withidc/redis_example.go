package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
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
		Addr:     "10.12.36.4:4025",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
}

func ExampleClientOp() {

	//client := redis.NewClient(&redis.Options{
	//	Addr:     "10.12.36.4:4025",
	//	Password: "", // no password set
	//	DB:       0,  // use default DB
	//})

	client := redis.NewPlusClient(&redis.FailoverOptions{
		MasterName:            "TestDBARedis001_001",
		SentinelAddrs:         []string{"XX.XX.XX.XX:20019"},
		SentinelPassword:      "",
		RouteByLatency:        false,
		RouteRandomly:         true,
		SlaveOnly:             true,
		Rws:                   true,
		IdcId:                 "1234",
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

	err := client.Set(ctx, "key", "value4", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val, err = client.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)
	/*
		val2, err := client.Get(ctx, "key2").Result()
		if err == redis.Nil {
			fmt.Println("key2 does not exist")
		} else if err != nil {
			panic(err)
		} else {
			fmt.Println("key2", val2)
		}
	*/
	//time.Sleep(1000 * time.Second)
	// Output: key value
	// key2 does not exist
}
