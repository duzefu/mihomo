package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
)

// OptimizedURLTest 优化版本的URLTest，支持快速故障切换
type OptimizedURLTest struct {
	*GroupBase
	selected       string
	testUrl        string
	expectedStatus string
	tolerance      uint16
	disableUDP     bool
	Hidden         bool
	Icon           string
	fastNode       C.Proxy
	fastSingle     *singledo.Single[C.Proxy]

	// 新增字段用于快速故障切换
	quickCheckPercent float64       // 快速检查的节点百分比阈值（默认0.3，即30%）
	quickCheckTimeout time.Duration // 快速检查超时时间
	isQuickChecking   atomic.Bool   // 是否正在进行快速检查
	quickCheckMux     sync.Mutex    // 快速检查互斥锁
}

func (u *OptimizedURLTest) Now() string {
	return u.fast(false).Name()
}

func (u *OptimizedURLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}
	if p == nil {
		return errors.New("proxy not exist")
	}
	u.ForceSet(name)
	return nil
}

func (u *OptimizedURLTest) ForceSet(name string) {
	u.selected = name
	u.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (u *OptimizedURLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy := u.fast(true)
	c, err = proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(u)
	} else {
		// 修改这里：失败时立即触发快速检查而不是等待多次失败
		u.onDialFailed(proxy.Type(), err, u.quickHealthCheck)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				u.onDialSuccess()
			} else {
				u.onDialFailed(proxy.Type(), err, u.quickHealthCheck)
			}
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (u *OptimizedURLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := u.fast(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.quickHealthCheck)
	}

	return pc, err
}

// Unwrap implements C.ProxyAdapter
func (u *OptimizedURLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
}

// 重写healthCheck方法，保持原有行为不变
func (u *OptimizedURLTest) healthCheck() {
	u.fastSingle.Reset()
	u.GroupBase.healthCheck()
	u.fastSingle.Reset()
}

// 新增快速健康检查方法
func (u *OptimizedURLTest) quickHealthCheck() {
	// 避免重复的快速检查
	if u.isQuickChecking.Load() {
		return
	}

	go u.performQuickCheck()
}

// 执行快速检查：检查前N个节点，一旦有1+30%的节点完成检查就立即选择最优的
func (u *OptimizedURLTest) performQuickCheck() {
	u.quickCheckMux.Lock()
	defer u.quickCheckMux.Unlock()

	if u.isQuickChecking.Load() {
		return
	}

	u.isQuickChecking.Store(true)
	defer u.isQuickChecking.Store(false)

	log.Debugln("OptimizedURLTest: starting quick health check for group %s", u.Name())

	proxies := u.GetProxies(false)
	if len(proxies) == 0 {
		return
	}

	// 限制检查的节点数量，避免检查太多节点
	checkCount := len(proxies)

	// 计算动态阈值：至少1个节点，且达到30%的节点数量
	minThreshold := 1
	percentThreshold := int(float64(checkCount) * u.quickCheckPercent)
	if percentThreshold < minThreshold {
		percentThreshold = minThreshold
	}

	log.Debugln("OptimizedURLTest: checking %d nodes, threshold: %d (%.0f%%)",
		checkCount, percentThreshold, u.quickCheckPercent*100)

	ctx, cancel := context.WithTimeout(context.Background(), u.quickCheckTimeout)
	defer cancel()

	type result struct {
		proxy C.Proxy
		delay uint16
		err   error
		index int
	}

	results := make(chan result, checkCount)
	var wg sync.WaitGroup

	// 并发测试前N个节点
	for i := 0; i < checkCount; i++ {
		wg.Add(1)
		go func(idx int, proxy C.Proxy) {
			defer wg.Done()

			expectedStatus, _ := utils.NewUnsignedRanges[uint16](u.expectedStatus)
			delay, err := proxy.URLTest(ctx, u.testUrl, expectedStatus)

			results <- result{
				proxy: proxy,
				delay: delay,
				err:   err,
				index: idx,
			}
		}(i, proxies[i])
	}

	// 等待结果或达到阈值
	go func() {
		wg.Wait()
		close(results)
	}()

	var validResults []result
	var bestResult *result

	// 收集结果，一旦达到阈值就立即处理
	for res := range results {
		if res.err == nil {
			validResults = append(validResults, res)

			// 更新最佳结果
			if bestResult == nil || res.delay < bestResult.delay {
				bestResult = &res
			}

			log.Debugln("OptimizedURLTest: proxy %s delay: %dms", res.proxy.Name(), res.delay)

			// 达到快速检查阈值，立即选择最优节点
			if len(validResults) >= percentThreshold {
				log.Infoln("OptimizedURLTest: quick check completed with %d/%d results (%.0f%%), switching to %s (delay: %dms)",
					len(validResults), checkCount, float64(len(validResults))/float64(checkCount)*100, bestResult.proxy.Name(), bestResult.delay)

				// 立即更新最快节点
				u.fastNode = bestResult.proxy
				u.fastSingle.Reset()
				return
			}
		}
	}

	// 如果没有达到阈值但有可用结果，也选择最优的
	if bestResult != nil {
		log.Infoln("OptimizedURLTest: quick check timeout with %d/%d results (%.0f%%), switching to %s (delay: %dms)",
			len(validResults), checkCount, float64(len(validResults))/float64(checkCount)*100, bestResult.proxy.Name(), bestResult.delay)

		u.fastNode = bestResult.proxy
		u.fastSingle.Reset()
	} else {
		log.Warnln("OptimizedURLTest: quick check failed, no available proxies found")
	}
}

func (u *OptimizedURLTest) fast(touch bool) C.Proxy {
	elm, _, shared := u.fastSingle.Do(func() (C.Proxy, error) {
		proxies := u.GetProxies(touch)
		if u.selected != "" {
			for _, proxy := range proxies {
				if !proxy.AliveForTestUrl(u.testUrl) {
					continue
				}
				if proxy.Name() == u.selected {
					u.fastNode = proxy
					return proxy, nil
				}
			}
		}

		fast := proxies[0]
		minDelay := fast.LastDelayForTestUrl(u.testUrl)
		fastNotExist := true

		for _, proxy := range proxies[1:] {
			if u.fastNode != nil && proxy.Name() == u.fastNode.Name() {
				fastNotExist = false
			}

			if !proxy.AliveForTestUrl(u.testUrl) {
				continue
			}

			delay := proxy.LastDelayForTestUrl(u.testUrl)
			if delay < minDelay {
				fast = proxy
				minDelay = delay
			}
		}

		// tolerance
		if u.fastNode == nil || fastNotExist || !u.fastNode.AliveForTestUrl(u.testUrl) || u.fastNode.LastDelayForTestUrl(u.testUrl) > fast.LastDelayForTestUrl(u.testUrl)+u.tolerance {
			u.fastNode = fast
		}
		return u.fastNode, nil
	})
	if shared && touch {
		u.Touch()
	}

	return elm
}

// SupportUDP implements C.ProxyAdapter
func (u *OptimizedURLTest) SupportUDP() bool {
	if u.disableUDP {
		return false
	}
	return u.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (u *OptimizedURLTest) IsL3Protocol(metadata *C.Metadata) bool {
	return u.fast(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (u *OptimizedURLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":              u.Type().String(),
		"now":               u.Now(),
		"all":               all,
		"testUrl":           u.testUrl,
		"expectedStatus":    u.expectedStatus,
		"fixed":             u.selected,
		"hidden":            u.Hidden,
		"icon":              u.Icon,
		"quickCheckPercent": u.quickCheckPercent,
	})
}

func (u *OptimizedURLTest) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return u.GroupBase.URLTest(ctx, u.testUrl, expectedStatus)
}

type optimizedUrlTestOption func(*OptimizedURLTest)

func optimizedUrlTestWithTolerance(tolerance uint16) optimizedUrlTestOption {
	return func(u *OptimizedURLTest) {
		u.tolerance = tolerance
	}
}

func optimizedUrlTestWithQuickCheck(percent float64, timeout time.Duration) optimizedUrlTestOption {
	return func(u *OptimizedURLTest) {
		u.quickCheckPercent = percent
		u.quickCheckTimeout = timeout
	}
}

func parseOptimizedURLTestOption(config map[string]any) []optimizedUrlTestOption {
	opts := []optimizedUrlTestOption{}

	// tolerance
	if elm, ok := config["tolerance"]; ok {
		if tolerance, ok := elm.(int); ok {
			opts = append(opts, optimizedUrlTestWithTolerance(uint16(tolerance)))
		}
	}

	// quick check percent
	percent := 0.3 // 默认30%
	if elm, ok := config["quick-check-percent"]; ok {
		if p, ok := elm.(float64); ok && p > 0 && p <= 1.0 {
			percent = p
		} else if i, ok := elm.(int); ok && i > 0 && i <= 100 {
			percent = float64(i) / 100.0
		}
	}

	// quick check timeout
	timeout := time.Second * 3 // 默认3秒
	if elm, ok := config["quick-check-timeout"]; ok {
		if t, ok := elm.(int); ok && t > 0 {
			timeout = time.Duration(t) * time.Millisecond
		}
	}

	opts = append(opts, optimizedUrlTestWithQuickCheck(percent, timeout))

	return opts
}

func NewOptimizedURLTest(option *GroupCommonOption, providers []provider.ProxyProvider, options ...optimizedUrlTestOption) *OptimizedURLTest {
	urlTest := &OptimizedURLTest{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.URLTest,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: 1, // 设置为1，即第一次失败就触发快速检查
			Providers:      providers,
		}),
		fastSingle:        singledo.NewSingle[C.Proxy](time.Second * 10),
		disableUDP:        option.DisableUDP,
		testUrl:           option.URL,
		expectedStatus:    option.ExpectedStatus,
		Hidden:            option.Hidden,
		Icon:              option.Icon,
		quickCheckPercent: 0.3,             // 默认30%的节点
		quickCheckTimeout: time.Second * 3, // 默认3秒超时
	}

	for _, option := range options {
		option(urlTest)
	}

	return urlTest
}
