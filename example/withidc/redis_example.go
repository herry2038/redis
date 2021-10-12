package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/example/common"
	"time"
)

var (
	//ctx = context.WithValue(context.Background(), "valMap", make(map[string]string))
	ctx = context.Background()
)

func main() {
	//ExampleNewClient()
	//ExampleClientOp()

	PressTest()
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

	client := common.GetClient()

	err := client.Set(ctx, "key", "value4", 0).Err()
	if err != nil {
		panic(err)
	}
	PrintInstance("set key")

	val, err := client.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)
	PrintInstance("get key")

	val, err = client.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	PrintInstance("get key  twice")

	val2, err := client.Get(ctx, "key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}

	PrintInstance("get key2")

	//time.Sleep(1000 * time.Second)
	// Output: key value
	// key2 does not exist
}

func PrintInstance(prefix string) {
	valmap := ctx.Value("valMap")
	if valmap != nil {
		if valMap2, ok := valmap.(map[string]string); ok {
			val := valMap2["instance"]
			fmt.Printf("%s: last instance: %s\n", prefix, val)
			return
		}
	}

	fmt.Printf("%s: last instance: unknown\n", prefix)
}

func GetLastExecInstance() string {
	valmap := ctx.Value("valMap")
	if valmap != nil {
		if valMap2, ok := valmap.(map[string]string); ok {
			val := valMap2["instance"]
			return val
		}
	}
	return ""
}

func PressTest() {
	{
		client := common.GetClient()
		statistics := make(map[string]int)
		for i := 0; i < 10; i++ {
			client.Get(ctx, "key")
		}

		start := time.Now()

		totalCnt := 10000
		for i := 0; i < totalCnt; i++ {
			client.Get(ctx, "key")
			instance := GetLastExecInstance()
			cnt, ok := statistics[instance]
			if ok {
				statistics[instance] = cnt + 1
			} else {
				statistics[instance] = 1
			}
		}

		usedSeconds := time.Now().Sub(start).Seconds()
		fmt.Printf("total executed: %d, used: %.2f\n", totalCnt, usedSeconds)
		for instance, cnt := range statistics {
			fmt.Printf("instance: %s executed: %d\n", instance, cnt)
		}
	}

	{
		client := common.GetMasterClient()
		statistics := make(map[string]int)
		for i := 0; i < 10; i++ {
			client.Get(ctx, "key")
		}

		start := time.Now()

		totalCnt := 10000
		for i := 0; i < totalCnt; i++ {
			client.Get(ctx, "key")
			instance := GetLastExecInstance()
			cnt, ok := statistics[instance]
			if ok {
				statistics[instance] = cnt + 1
			} else {
				statistics[instance] = 1
			}
		}

		usedSeconds := time.Now().Sub(start).Seconds()
		fmt.Printf("total executed: %d, used: %.2f\n", totalCnt, usedSeconds)
		for instance, cnt := range statistics {
			fmt.Printf("instance: %s executed: %d\n", instance, cnt)
		}
	}
}
