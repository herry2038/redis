package redis

import (
	"context"
	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/pool"
	"math/rand"
	"time"
)

type PlusClient struct {
	*clusterClient
	cmdable
	hooks
	ctx context.Context
}

func NewPlusClient(failoverOpt *FailoverOptions) *PlusClient {
	sentinelAddrs := make([]string, len(failoverOpt.SentinelAddrs))
	copy(sentinelAddrs, failoverOpt.SentinelAddrs)

	failover := &sentinelFailover{
		opt:           failoverOpt,
		sentinelAddrs: sentinelAddrs,
	}

	opt := failoverOpt.clusterOptions()
	opt.ClusterSlots = func(ctx context.Context) ([]ClusterSlot, error) {
		masterAddr, err := failover.MasterAddrPlus(ctx)
		if err != nil {
			return nil, err
		}

		nodes := []ClusterNode{{
			Addr: masterAddr,
		}}

		slaveAddrs, err := failover.slaveAddrsPlus(ctx, false)
		if err != nil {
			return nil, err
		}

		for _, slaveAddr := range slaveAddrs {
			nodes = append(nodes, ClusterNode{
				Addr: slaveAddr,
			})
		}

		slots := []ClusterSlot{
			{
				Start: 0,
				End:   16383,
				Nodes: nodes,
			},
		}
		return slots, nil
	}

	c := newPlusClient(opt)

	failover.mu.Lock()
	failover.onUpdate = func(ctx context.Context) {
		c.ReloadState(ctx)
	}
	failover.mu.Unlock()

	return c
}

func newPlusClient(opt *ClusterOptions) *PlusClient {
	opt.init()

	c := &PlusClient{
		clusterClient: &clusterClient{
			opt:   opt,
			nodes: newClusterNodes(opt),
		},
		ctx: context.Background(),
	}
	c.state = newClusterStateHolder(c.loadState)
	c.cmdsInfoCache = newCmdsInfoCache(c.cmdsInfo)
	c.cmdable = c.Process

	if opt.IdleCheckFrequency > 0 {
		go c.reaper(opt.IdleCheckFrequency)
	}

	return c
}

func (c *PlusClient) loadState(ctx context.Context) (*clusterState, error) {
	if c.opt.ClusterSlots != nil {
		slots, err := c.opt.ClusterSlots(ctx)
		if err != nil {
			return nil, err
		}
		return newClusterState(c.nodes, slots, "")
	}

	addrs, err := c.nodes.Addrs()
	if err != nil {
		return nil, err
	}

	var firstErr error

	for _, idx := range rand.Perm(len(addrs)) {
		addr := addrs[idx]

		node, err := c.nodes.Get(addr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		slots, err := node.Client.ClusterSlots(ctx).Result()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		return newClusterState(c.nodes, slots, node.Client.opt.Addr)
	}

	/*
	 * No node is connectable. It's possible that all nodes' IP has changed.
	 * Clear activeAddrs to let client be able to re-connect using the initial
	 * setting of the addresses (e.g. [redis-cluster-0:6379, redis-cluster-1:6379]),
	 * which might have chance to resolve domain name and get updated IP address.
	 */
	c.nodes.mu.Lock()
	c.nodes.activeAddrs = nil
	c.nodes.mu.Unlock()

	return nil, firstErr
}

func (c *PlusClient) Context() context.Context {
	return c.ctx
}

func (c *PlusClient) cmdInfo(name string) *CommandInfo {
	cmdsInfo, err := c.cmdsInfoCache.Get(c.ctx)
	if err != nil {
		return nil
	}

	info := cmdsInfo[name]
	if info == nil {
		internal.Logger.Printf(c.Context(), "info for cmd=%s not found", name)
	}
	return info
}

func (c *PlusClient) Process(ctx context.Context, cmd Cmder) error {
	return c.hooks.process(ctx, cmd, c.process)
}

func (c *PlusClient) process(ctx context.Context, cmd Cmder) error {
	cmdInfo := c.cmdInfo(cmd.Name())
	slot := c.cmdSlot(cmd)

	var node *clusterNode
	var ask bool
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		if node == nil {
			var err error
			node, err = c.cmdNodePlus(ctx, cmdInfo, slot, cmd.ExecMode())
			if err != nil {
				return err
			}
		}
		valMap := ctx.Value("valMap")
		if valMap != nil {
			if valMap2, ok := valMap.(map[string]string); ok {
				valMap2["instance"] = node.Client.opt.Addr
			}
		}

		if ask {
			pipe := node.Client.Pipeline()
			_ = pipe.Process(ctx, NewCmd(ctx, "asking"))
			_ = pipe.Process(ctx, cmd)
			_, lastErr = pipe.Exec(ctx)
			_ = pipe.Close()
			ask = false
		} else {
			lastErr = node.Client.Process(ctx, cmd)
		}

		// If there is no error - we are done.
		if lastErr == nil {
			return nil
		}
		if isReadOnly := isReadOnlyError(lastErr); isReadOnly || lastErr == pool.ErrClosed {
			if isReadOnly {
				c.state.LazyReload()
			}
			node = nil
			continue
		}

		// If slave is loading - pick another node.
		if c.opt.ReadOnly && isLoadingError(lastErr) {
			node.MarkAsFailing()
			node = nil
			continue
		}

		var moved bool
		var addr string
		moved, ask, addr = isMovedError(lastErr)
		if moved || ask {
			var err error
			node, err = c.nodes.Get(addr)
			if err != nil {
				return err
			}
			continue
		}

		if shouldRetry(lastErr, cmd.readTimeout() == nil) {
			// First retry the same node.
			if attempt == 0 {
				continue
			}

			// Second try another node.
			node.MarkAsFailing()
			node = nil
			continue
		}

		return lastErr
	}
	return lastErr
}

func (c *PlusClient) cmdSlot(cmd Cmder) int {
	args := cmd.Args()
	if args[0] == "cluster" && args[1] == "getkeysinslot" {
		return args[2].(int)
	}

	cmdInfo := c.cmdInfo(cmd.Name())
	return cmdSlot(cmd, cmdFirstKeyPos(cmd, cmdInfo))
}

func (c *PlusClient) retryBackoff(attempt int) time.Duration {
	return internal.RetryBackoff(attempt, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
}

func (c *PlusClient) cmdNode(
	ctx context.Context,
	cmdInfo *CommandInfo,
	slot int,
) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}

	if c.opt.ReadOnly && cmdInfo != nil && cmdInfo.ReadOnly {
		return c.slotReadOnlyNode(state, slot)
	}
	return state.slotMasterNode(slot)
}

func (c *PlusClient) cmdNodePlus(
	ctx context.Context,
	cmdInfo *CommandInfo,
	slot int,
	mode ExecMODE,
) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}

	if c.opt.ReadOnly && cmdInfo != nil && cmdInfo.ReadOnly {
		switch mode {
		case MODE_DEFAULT:
			return c.slotReadOnlyNode(state, slot)
		case MODE_CONSISTENCY:
			return state.slotMasterNode(slot)
		case MODE_IDC_RWS:
			return state.slotSameIdcNode(slot)
		case MODE_LATENCY_RWS:
			return state.slotClosestNode(slot)
		case MODE_RAMDOM_RWS:
			return state.slotRandomNode(slot)
		}
	}
	return state.slotMasterNode(slot)
}

func (c *PlusClient) cmdsInfo(ctx context.Context) (map[string]*CommandInfo, error) {
	// Try 3 random nodes.
	const nodeLimit = 3

	addrs, err := c.nodes.Addrs()
	if err != nil {
		return nil, err
	}

	var firstErr error

	perm := rand.Perm(len(addrs))
	if len(perm) > nodeLimit {
		perm = perm[:nodeLimit]
	}

	for _, idx := range perm {
		addr := addrs[idx]

		node, err := c.nodes.Get(addr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		info, err := node.Client.Command(ctx).Result()
		if err == nil {
			return info, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr == nil {
		panic("not reached")
	}
	return nil, firstErr
}

// reaper closes idle connections to the cluster.
func (c *PlusClient) reaper(idleCheckFrequency time.Duration) {
	ticker := time.NewTicker(idleCheckFrequency)
	defer ticker.Stop()

	for range ticker.C {
		nodes, err := c.nodes.All()
		if err != nil {
			break
		}

		for _, node := range nodes {
			_, err := node.Client.connPool.(*pool.ConnPool).ReapStaleConns()
			if err != nil {
				internal.Logger.Printf(c.Context(), "ReapStaleConns failed: %s", err)
			}
		}
	}
}

// ReloadState reloads cluster state. If available it calls ClusterSlots func
// to get cluster slots information.
func (c *PlusClient) ReloadState(ctx context.Context) {
	c.state.LazyReload()
}

// Do creates a Cmd from the args and processes the cmd.
func (c *PlusClient) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Do creates a Cmd from the args and processes the cmd.
func (c *PlusClient) DoPlus(ctx context.Context, mode ExecMODE, args ...interface{}) *Cmd {
	cmd := NewCmdPlus(ctx, mode, args...)

	_ = c.Process(ctx, cmd)
	return cmd
}

// PoolStats returns accumulated connection pool stats.
func (c *PlusClient) PoolStats() *PoolStats {
	var acc PoolStats

	state, _ := c.state.Get(context.TODO())
	if state == nil {
		return &acc
	}

	for _, node := range state.Masters {
		s := node.Client.connPool.Stats()
		acc.Hits += s.Hits
		acc.Misses += s.Misses
		acc.Timeouts += s.Timeouts

		acc.TotalConns += s.TotalConns
		acc.IdleConns += s.IdleConns
		acc.StaleConns += s.StaleConns
	}

	for _, node := range state.Slaves {
		s := node.Client.connPool.Stats()
		acc.Hits += s.Hits
		acc.Misses += s.Misses
		acc.Timeouts += s.Timeouts

		acc.TotalConns += s.TotalConns
		acc.IdleConns += s.IdleConns
		acc.StaleConns += s.StaleConns
	}

	return &acc
}

func (c *PlusClient) Options() *ClusterOptions {
	return c.opt
}
