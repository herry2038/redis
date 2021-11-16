package main

import (
	"fmt"
	"github.com/go-redis/redis/v8/example/common"
)

type CommandInfo struct {
	Name        string
	Arity       int8
	Flags       []string
	ACLFlags    []string
	FirstKeyPos int8
	LastKeyPos  int8
	StepCount   int8
	ReadOnly    bool
}

func PrintfCommandInfos() {
	client := common.GetPlusClient()
	cmds, err := client.Command(ctx).Result()
	if err != nil {
		panic(err)
	}

	f := func(vals []string) string {
		s := ""
		for _, val := range vals {
			s += fmt.Sprintf("\"%s\", ", val)
		}
		return s
	}

	fmt.Println("[]redis.CommandInfo{")
	for _, cmd := range cmds {
		fmt.Printf("\tredis.CommandInfo{\"%s\", %d, []string{%s}, []string{%s}, %d, %d, %d, %t},\n", cmd.Name, cmd.Arity, f(cmd.Flags), f(cmd.ACLFlags), cmd.FirstKeyPos, cmd.LastKeyPos, cmd.StepCount, cmd.ReadOnly)
	}
	fmt.Println("}")
}

func Test() {
	c := []CommandInfo{
		CommandInfo{"abc", 2, []string{"readonly"}, []string{"acl"}, 0, 2, 0, true},
	}

	fmt.Printf("%v", c)
}
