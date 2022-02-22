package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8/example/common"
)

var (
	ctx = context.Background()
)

func main() {
	//ExampleNewClient()
	//ExampleClientOp()
	ExampleWithVersion()
}

func ExampleNewClient() {
	client := common.GetDirectClient()
	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)

	err = client.Set(ctx, "key", "value4", 0).Err()
	if err != nil {
		panic(err)
	}
	// Output: PONG <nil>
}

func ExampleWithVersion() {
	client := common.GetDirectClient()
	var err error
	//err = client.SetWithVersion(ctx, "a", "e", 0, 862726210708836865).Err()
	//if err != nil {
	//	panic(err)
	//}

	err = client.Dev(ctx, "a", 862727048023913089).Err()
	if err != nil {
		panic(err)
	}
}

func ExampleClientOp() {

	//err := client.MGet(ctx, "key", "value4", "value3", "value3").Err()
	//if err != nil {
	//	panic(err)
	//}

	client := common.GetDirectClient()
	err := client.Set(ctx, "key", "value4", 0).Err()
	if err != nil {
		panic(err)
	}

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
