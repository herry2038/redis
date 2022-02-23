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
	err = client.SetWithVersion(ctx, "a", "e", 0, 862726210708836865).Err()
	if err != nil {
		panic(err)
	}

	err = client.Dev(ctx, "a", 862727048023913089).Err()
	if err != nil {
		panic(err)
	}
}
