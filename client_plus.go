package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/pool"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type PlusClient struct {
	*clusterClient
	cmdable
	hooks
	ctx context.Context
}

func NewPlusClient(failoverOpt *FailoverOptions) *PlusClient {
	return NewPlusClientEx(failoverOpt, nil)
}

func NewPlusClientEx(failoverOpt *FailoverOptions, loadCmdsFunc func(context.Context) (map[string]*CommandInfo, error)) *PlusClient {
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

	c := newPlusClient(opt, loadCmdsFunc)

	failover.mu.Lock()
	failover.onUpdate = func(ctx context.Context) {
		c.ReloadState(ctx)
	}
	failover.mu.Unlock()

	return c
}

func newPlusClient(opt *ClusterOptions, loadCmdsFunc func(context.Context) (map[string]*CommandInfo, error)) *PlusClient {
	opt.init()

	c := &PlusClient{
		clusterClient: &clusterClient{
			opt:   opt,
			nodes: newClusterNodes(opt),
		},
		ctx: context.Background(),
	}
	c.state = newClusterStateHolder(c.loadState)
	f := loadCmdsFunc
	if f == nil {
		f = c.cmdsInfo
	}
	c.cmdsInfoCache = newCmdsInfoCache(f)
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
			node, err = c.cmdNodePlus(ctx, cmdInfo, slot, cmd)
			if err != nil {
				return err
			}
		}
		/*
			valMap := ctx.Value("valMap")
			var valMap2 map[string]string
			if valMap != nil {
				valMap2, _ = valMap.(map[string]string)

			}
			if valMap2 != nil {
				valMap2["instance"] = node.Client.opt.Addr
			}
		*/

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

func IsScanCursorZero(cursor interface{}) (bool, error) {
	var val uint64
	var err error
	switch v := cursor.(type) {
	case uint:
		val = uint64(v)
	case int:
		val = uint64(v)
	case int8:
		val = uint64(v)
	case uint8:
		val = uint64(v)
	case int16:
		val = uint64(v)
	case uint16:
		val = uint64(v)
	case int32:
		val = uint64(v)
	case uint32:
		val = uint64(v)
	case int64:
		val = uint64(v)
	case uint64:
		val = uint64(v)
	case string:
		val, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return false, err
		}
	case []byte:
		val, err = strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return false, err
		}
	default:
		return false, err
	}
	return val == 0, nil
}

func isScanCursorZero(scan *ScanCmd) (bool, error) {
	if len(scan.args) < 2 {
		return false, nil
	}
	pos := 1
	switch val := scan.args[0].(type) {
	case []byte:
		if val[0] == 's' || val[0] == 'S' {
			if val[1] == 's' || val[1] == 'S' {
				if len(scan.args) < 3 {
					return false, nil
				}
				pos = 2
			}
		} else if val[0] == 'h' || val[0] == 'H' {
			if len(scan.args) < 3 {
				return false, nil
			}
			pos = 2
		}
	case string:
		if val[0] == 's' || val[0] == 'S' {
			if val[1] == 's' || val[1] == 'S' {
				if len(scan.args) < 3 {
					return false, nil
				}
				pos = 2
			}
		} else if val[0] == 'h' || val[0] == 'H' {
			if len(scan.args) < 3 {
				return false, nil
			}
			pos = 2
		}
	default:
		return false, nil
	}

	var val uint64
	var err error
	switch v := scan.args[pos].(type) {
	case uint:
		val = uint64(v)
	case int:
		val = uint64(v)
	case int8:
		val = uint64(v)
	case uint8:
		val = uint64(v)
	case int16:
		val = uint64(v)
	case uint16:
		val = uint64(v)
	case int32:
		val = uint64(v)
	case uint32:
		val = uint64(v)
	case int64:
		val = uint64(v)
	case uint64:
		val = uint64(v)
	case string:
		val, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return false, err
		}
	case []byte:
		val, err = strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return false, err
		}
	default:
		return false, err
	}
	return val == 0, nil
}

func scanCursorPos(cmdInfo *CommandInfo, cmd Cmder) int {
	pos := 0
	if cmdInfo != nil {
		if cmdInfo.Name == "scan" {
			pos = 1
		} else if cmdInfo.Name == "sscan" || cmdInfo.Name == "hscan" {
			pos = 2
		}
	} else if scan, ok := cmd.(*ScanCmd); ok {
		cmdName := scan.args[0]
		switch val := cmdName.(type) {
		case []byte:
			if val[0] == 's' || val[0] == 'S' {
				if val[1] == 's' || val[1] == 'S' {
					pos = 2
				} else {
					pos = 1
				}
			} else if val[0] == 'h' || val[0] == 'H' {
				pos = 2
			}
		case string:
			if val[0] == 's' || val[0] == 'S' {
				if val[1] == 's' || val[1] == 'S' {
					pos = 2
				} else {
					pos = 1
				}
			} else if val[0] == 'h' || val[0] == 'H' {
				pos = 2
			}
		default:
			pos = 0
		}
	} else {
		switch val := cmd.Args()[0].(type) {
		case []byte:
			cmdName := strings.ToLower(string(val))
			switch cmdName {
			case "sscan", "hscan":
				pos = 2
			case "scan":
				pos = 1
			}
		case string:
			cmdName := strings.ToLower(val)
			switch cmdName {
			case "sscan", "hscan":
				pos = 2
			case "scan":
				pos = 1
			}
		default:
			pos = 0
		}
	}
	return pos
}

func (c *PlusClient) cmdNodePlus(
	ctx context.Context,
	cmdInfo *CommandInfo,
	slot int,
	cmd Cmder,
) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	var node *clusterNode
	isFirstScan := false
	valMap := ctx.Value("valMap")
	var valMap2 map[string]string
	if valMap != nil {
		valMap2, _ = valMap.(map[string]string)
	}

	pos := scanCursorPos(cmdInfo, cmd)
	if pos > 0 {
		if len(cmd.Args()) <= pos {
			return nil, errors.New("invalid cmd args!")
		}
		var ok bool
		ok, err = IsScanCursorZero(cmd.Args()[pos])
		if err == nil { // 判断游标时，err为nil才考虑
			if ok {
				isFirstScan = true
			} else {
				if nodeAddr, nodeExists := valMap2["node"]; nodeExists {
					node, err = state.nodes.Get(nodeAddr)
					if err != nil {
						return node, err
					}
				}
			}
		}
	}

	if node == nil {
		if c.opt.ReadOnly && cmdInfo != nil && cmdInfo.ReadOnly {
			switch cmd.ExecMode() {
			case MODE_DEFAULT:
				node, err = c.slotReadOnlyNode(state, slot)
			case MODE_CONSISTENCY:
				node, err = state.slotMasterNode(slot)
			case MODE_IDC_RWS:
				node, err = state.slotSameIdcNode(slot)
			case MODE_LATENCY_RWS:
				node, err = state.slotClosestNode(slot)
			case MODE_RAMDOM_RWS:
				node, err = state.slotRandomNode(slot)
			default:
				node, err = state.slotMasterNode(slot)
			}
		} else {
			node, err = state.slotMasterNode(slot)
		}
	}

	if valMap2 != nil && node != nil {
		valMap2["instance"] = node.Client.opt.Addr
		if isFirstScan {
			valMap2["node"] = node.Client.opt.Addr
		}
	}
	return node, err
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
