package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8/internal"
	"strings"
)

func (c *sentinelFailover) MasterAddrPlus(ctx context.Context) (string, error) {
	c.mu.RLock()
	sentinel := c.sentinel
	c.mu.RUnlock()

	if sentinel != nil {
		addr := c.getMasterAddrPlus(ctx, sentinel)
		if addr != "" {
			return addr, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sentinel != nil {
		addr := c.getMasterAddrPlus(ctx, c.sentinel)
		if addr != "" {
			return addr, nil
		}
		_ = c.closeSentinel()
	}

	for i, sentinelAddr := range c.sentinelAddrs {
		sentinel := NewSentinelClient(c.opt.sentinelOptions(sentinelAddr))

		mInfo, err := sentinel.Master(ctx, c.opt.MasterName).Result()
		if err != nil {
			internal.Logger.Printf(ctx, "sentinel: GetMasterAddrByName master=%q failed: %s",
				c.opt.MasterName, err)
			_ = sentinel.Close()
			continue
		}

		// Push working sentinel to the top.
		c.sentinelAddrs[0], c.sentinelAddrs[i] = c.sentinelAddrs[i], c.sentinelAddrs[0]
		c.setSentinel(ctx, sentinel)

		idcId := mInfo["idc_id"]
		ip := mInfo["ip"]
		port := mInfo["port"]
		return ip + ":" + strings.TrimSpace(port) + ":" + idcId, nil
	}

	return "", errors.New("redis: all sentinels specified in configuration are unreachable")
}

func (c *sentinelFailover) getMasterAddrPlus(ctx context.Context, sentinel *SentinelClient) string {
	mInfo, err := sentinel.Master(ctx, c.opt.MasterName).Result()
	if err != nil {
		internal.Logger.Printf(ctx, "sentinel: Master name=%q failed: %s",
			c.opt.MasterName, err)
		return ""
	}

	idcId := mInfo["idc_id"]
	ip := mInfo["ip"]
	port := mInfo["port"]
	return ip + ":" + strings.TrimSpace(port) + ":" + idcId
}

func (c *sentinelFailover) slaveAddrsPlus(ctx context.Context, useDisconnected bool) ([]string, error) {
	c.mu.RLock()
	sentinel := c.sentinel
	c.mu.RUnlock()

	if sentinel != nil {
		addrs := c.getSlaveAddrsPlus(ctx, sentinel)
		if len(addrs) > 0 {
			return addrs, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sentinel != nil {
		addrs := c.getSlaveAddrsPlus(ctx, c.sentinel)
		if len(addrs) > 0 {
			return addrs, nil
		}
		_ = c.closeSentinel()
	}

	var sentinelReachable bool

	for i, sentinelAddr := range c.sentinelAddrs {
		sentinel := NewSentinelClient(c.opt.sentinelOptions(sentinelAddr))

		slaves, err := sentinel.Slaves(ctx, c.opt.MasterName).Result()
		if err != nil {
			internal.Logger.Printf(ctx, "sentinel: Slaves master=%q failed: %s",
				c.opt.MasterName, err)
			_ = sentinel.Close()
			continue
		}
		sentinelReachable = true
		addrs := parseSlaveAddrsPlus(slaves, useDisconnected)
		if len(addrs) == 0 {
			continue
		}
		// Push working sentinel to the top.
		c.sentinelAddrs[0], c.sentinelAddrs[i] = c.sentinelAddrs[i], c.sentinelAddrs[0]
		c.setSentinel(ctx, sentinel)

		return addrs, nil
	}

	if sentinelReachable {
		return []string{}, nil
	}
	return []string{}, errors.New("redis: all sentinels specified in configuration are unreachable")
}

func (c *sentinelFailover) getSlaveAddrsPlus(ctx context.Context, sentinel *SentinelClient) []string {
	addrs, err := sentinel.Slaves(ctx, c.opt.MasterName).Result()
	if err != nil {
		internal.Logger.Printf(ctx, "sentinel: Slaves name=%q failed: %s",
			c.opt.MasterName, err)
		return []string{}
	}
	return parseSlaveAddrsPlus(addrs, false)
}

func parseSlaveAddrsPlus(addrs []interface{}, keepDisconnected bool) []string {
	nodes := make([]string, 0, len(addrs))
	for _, node := range addrs {
		ip := ""
		port := ""
		idcId := ""
		flags := []string{}
		lastkey := ""
		isDown := false

		for _, key := range node.([]interface{}) {
			switch lastkey {
			case "ip":
				ip = key.(string)
			case "port":
				port = key.(string)
			case "idc_id":
				idcId = key.(string)
			case "flags":
				flags = strings.Split(key.(string), ",")
			}
			lastkey = key.(string)
		}

		for _, flag := range flags {
			switch flag {
			case "s_down", "o_down":
				isDown = true
			case "disconnected":
				if !keepDisconnected {
					isDown = true
				}
			}
		}

		if !isDown {
			nodes = append(nodes, ip+":"+strings.TrimSpace(port)+":"+idcId)
		}
	}
	return nodes
}
