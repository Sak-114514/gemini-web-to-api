package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"gemini-web-to-api/internal/commons/configs"

	"go.uber.org/zap"
)

// cookieFromHeader extracts one cookie value from a raw Cookie header.
func cookieFromHeader(header, wanted string) string {
	for _, part := range strings.Split(header, ";") {
		name, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if found && name == wanted {
			return value
		}
	}
	return ""
}

// PoolAccountConfig is one pooled Gemini Web account. ProxyURL is optional;
// empty means the process-wide GEMINI_PROXY_URL (or direct access).
type PoolAccountConfig struct {
	ID       string `json:"id"`
	Cookies  string `json:"cookies"`
	AuthUser string `json:"auth_user,omitempty"`
	ProxyURL string `json:"proxy_url,omitempty"`
}

// PoolSchedulerConfig controls how jobs spread across accounts. Every knob is
// optional; zero values fall back to the defaults below.
type PoolSchedulerConfig struct {
	// Selection picks among eligible accounts: "random" (default) or "first".
	Selection string `json:"selection,omitempty"`
	// MaxConcurrentPerAccount caps parallel jobs per account (default 3,
	// hard-capped at MaxConcurrentPerAccountCap, default 5 — the web client
	// visibly runs a handful of concurrent video sessions, more looks scripted).
	MaxConcurrentPerAccount int `json:"max_concurrent_per_account,omitempty"`
	MaxConcurrentPerAccountCap int `json:"max_concurrent_per_account_cap,omitempty"`
	// SubmitIntervalSecondsMin/Max space out the moments a job is actually
	// submitted upstream (default 30–120s, jittered). 0/0 disables pacing.
	SubmitIntervalSecondsMin int `json:"submit_interval_seconds_min,omitempty"`
	SubmitIntervalSecondsMax int `json:"submit_interval_seconds_max,omitempty"`
	// BotProtectionCooldownSeconds rests an account after a bot-protection
	// reply (default 180); the job itself waits, then continues on another
	// account.
	BotProtectionCooldownSeconds int `json:"bot_protection_cooldown_seconds,omitempty"`
	// QuotaExclusionHours parks an account that reported its video quota
	// exhausted (default 6, matching the daily-reset rhythm).
	QuotaExclusionHours int `json:"quota_exclusion_hours,omitempty"`
}

// PoolConfig is the file format of GEMINI_POOL_CONFIG.
type PoolConfig struct {
	Scheduler PoolSchedulerConfig `json:"scheduler"`
	Accounts  []PoolAccountConfig `json:"accounts"`
}

func (p *PoolSchedulerConfig) fillDefaults() {
	if p.Selection == "" {
		p.Selection = "random"
	}
	if p.MaxConcurrentPerAccount <= 0 {
		p.MaxConcurrentPerAccount = 3
	}
	if p.MaxConcurrentPerAccountCap <= 0 {
		p.MaxConcurrentPerAccountCap = 5
	}
	if p.MaxConcurrentPerAccount > p.MaxConcurrentPerAccountCap {
		p.MaxConcurrentPerAccount = p.MaxConcurrentPerAccountCap
	}
	if p.BotProtectionCooldownSeconds <= 0 {
		p.BotProtectionCooldownSeconds = 180
	}
	if p.QuotaExclusionHours <= 0 {
		p.QuotaExclusionHours = 6
	}
	if p.SubmitIntervalSecondsMin < 0 {
		p.SubmitIntervalSecondsMin = 0
	}
	if p.SubmitIntervalSecondsMax < p.SubmitIntervalSecondsMin {
		p.SubmitIntervalSecondsMax = p.SubmitIntervalSecondsMin
	}
}

// LoadPoolConfig reads and validates the pool file. It returns nil when path
// is empty so plain single-account deployments keep working unchanged.
func LoadPoolConfig(path string, log *zap.Logger) *PoolConfig {
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Error("Pool config unreadable; falling back to single account", zap.String("path", path), zap.Error(err))
		return nil
	}
	var config PoolConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		log.Error("Pool config invalid; falling back to single account", zap.String("path", path), zap.Error(err))
		return nil
	}
	seen := map[string]bool{}
	for i := range config.Accounts {
		account := &config.Accounts[i]
		if strings.TrimSpace(account.Cookies) == "" {
			log.Error("Pool account without cookies dropped", zap.Int("index", i), zap.String("id", account.ID))
			account.Cookies = ""
			continue
		}
		if account.ID == "" {
			account.ID = fmt.Sprintf("account_%d", i+1)
		}
		if seen[account.ID] {
			log.Error("Duplicate pool account id", zap.String("id", account.ID))
			account.Cookies = ""
			continue
		}
		seen[account.ID] = true
	}
	live := config.Accounts[:0]
	for _, account := range config.Accounts {
		if account.Cookies != "" {
			live = append(live, account)
		}
	}
	config.Accounts = live
	if len(config.Accounts) == 0 {
		log.Error("Pool config has no usable accounts; falling back to single account", zap.String("path", path))
		return nil
	}
	config.Scheduler.fillDefaults()
	return &config
}

// poolMember is one account with its scheduling state.
type poolMember struct {
	id     string
	client *Client

	mu        sync.Mutex
	active    int
	quotaUntil time.Time
	botUntil   time.Time
	healthy    bool
}

func (m *poolMember) available(now time.Time, maxConcurrent int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthy && m.active < maxConcurrent && now.After(m.quotaUntil) && now.After(m.botUntil)
}

func (m *poolMember) markActive(delta int) {
	m.mu.Lock()
	m.active += delta
	if m.active < 0 {
		m.active = 0
	}
	m.mu.Unlock()
}

func (m *poolMember) setQuotaUntil(t time.Time) {
	m.mu.Lock()
	m.quotaUntil = t
	m.mu.Unlock()
}

func (m *poolMember) setBotUntil(t time.Time) {
	m.mu.Lock()
	m.botUntil = t
	m.mu.Unlock()
}

func (m *poolMember) setHealthy(v bool) {
	m.mu.Lock()
	m.healthy = v
	m.mu.Unlock()
}

// ClientPool spreads traffic across several Gemini Web accounts. The facade
// Client routes GenerateContent/GenerateMusic/StartVideoJob through the pool;
// the first account reuses the facade itself so env-cookies keep working.
type ClientPool struct {
	members []*poolMember
	sched   PoolSchedulerConfig
	log     *zap.Logger

	submitMu     sync.Mutex
	lastSubmitAt time.Time
}

func newClientPool(config *PoolConfig, facade *Client, cfg *configs.Config, log *zap.Logger) *ClientPool {
	pool := &ClientPool{sched: config.Scheduler, log: log}
	for i, account := range config.Accounts {
		member := &poolMember{id: account.ID}
		if i == 0 {
			// The facade becomes account #1: rebind its session to the pool
			// entry so env/GEMINI_COOKIES no longer decides the traffic source.
			facade.rebindAccount(account.Cookies, account.AuthUser, account.ProxyURL, log)
			member.client = facade
			pool.members = append(pool.members, member)
			continue
		}
		accountCfg := *cfg
		accountCfg.Gemini.Cookies = account.Cookies
		accountCfg.Gemini.AuthUser = strings.TrimSpace(account.AuthUser)
		accountCfg.Gemini.Secure1PSID = cookieFromHeader(account.Cookies, "__Secure-1PSID")
		accountCfg.Gemini.Secure1PSIDTS = cookieFromHeader(account.Cookies, "__Secure-1PSIDTS")
		client := newClientCore(&accountCfg, log, nil, false)
		client.rebindAccount(account.Cookies, account.AuthUser, account.ProxyURL, log)
		member.client = client
		pool.members = append(pool.members, member)
	}
	return pool
}

// rebindAccount points a client at a pool account (cookies, slot, egress).
func (c *Client) rebindAccount(cookieHeader, authUser, proxyURL string, log *zap.Logger) {
	cookieHeader = strings.TrimSpace(cookieHeader)
	c.mu.Lock()
	c.configuredCookieHeader = cookieHeader
	c.authUser = strings.TrimSpace(authUser)
	c.proxyURL = strings.TrimSpace(proxyURL)
	c.cookies = &CookieStore{
		Secure1PSID:   cookieFromHeader(cookieHeader, "__Secure-1PSID"),
		Secure1PSIDTS: cookieFromHeader(cookieHeader, "__Secure-1PSIDTS"),
		UpdatedAt:     time.Now(),
		Source:        CookieSourceEnv,
	}
	c.mu.Unlock()
	if c.proxyURL != "" {
		c.httpClient.SetProxyURL(c.proxyURL)
		log.Info("Pool account bound", zap.String("auth_user", authUser), zap.String("proxy_url", c.proxyURL))
	} else {
		log.Info("Pool account bound", zap.String("auth_user", authUser))
	}
}

// pick returns a random (or first) eligible member, excluding skip.
func (p *ClientPool) pick(now time.Time, skip map[string]bool) *poolMember {
	var eligible []*poolMember
	for _, member := range p.members {
		if skip[member.id] {
			continue
		}
		if member.available(now, p.sched.MaxConcurrentPerAccount) {
			eligible = append(eligible, member)
		}
	}
	if len(eligible) == 0 {
		return nil
	}
	if p.sched.Selection == "first" || len(eligible) == 1 {
		return eligible[0]
	}
	return eligible[rand.Intn(len(eligible))]
}

// pickForChat routes single-shot generation traffic.
func (p *ClientPool) pickForChat() *Client {
	member := p.pick(time.Now(), nil)
	if member == nil {
		return nil
	}
	return member.client
}

// waitSubmitGate spaces out upstream submissions with a jittered interval.
func (p *ClientPool) waitSubmitGate(ctx context.Context) error {
	minSeconds := p.sched.SubmitIntervalSecondsMin
	maxSeconds := p.sched.SubmitIntervalSecondsMax
	if maxSeconds <= 0 && minSeconds <= 0 {
		return nil
	}
	p.submitMu.Lock()
	delay := time.Duration(0)
	if !p.lastSubmitAt.IsZero() {
		spread := maxSeconds
		if spread > minSeconds {
			spread = minSeconds + rand.Intn(spread-minSeconds)
		}
		next := p.lastSubmitAt.Add(time.Duration(spread) * time.Second)
		if delay = time.Until(next); delay < 0 {
			delay = 0
		}
	}
	p.lastSubmitAt = time.Now().Add(delay)
	p.submitMu.Unlock()
	if delay <= 0 {
		return nil
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isBotProtectionReply recognizes the web app's bot-protection / unusual
// traffic errors (BardErrorInfo 13 1152 family).
func isBotProtectionReply(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "1152") ||
		strings.Contains(text, "unusual traffic") ||
		strings.Contains(text, "CAPTCHA") ||
		strings.Contains(text, "bot protection")
}

// generateVideoOnPool runs one video job with full pool semantics: submit
// pacing, per-account concurrency, quota exclusion and bot-protection
// cooldown with an account switch. tried ids are never reused in one job.
func (p *ClientPool) generateVideoOnPool(ctx context.Context, prompt, model string, aspect VideoAspectRatio, files []InputFile) (*VideoResult, error) {
	if err := p.waitSubmitGate(ctx); err != nil {
		return nil, &VideoError{Code: VideoErrorTimeout, Message: "Job canceled while waiting for the submit gate: " + err.Error()}
	}
	tried := map[string]bool{}
	var lastErr error
	for len(tried) < len(p.members) {
		member := p.pick(time.Now(), tried)
		if member == nil {
			if lastErr == nil {
				lastErr = &VideoError{Code: VideoErrorQuota, Message: "No pool account is currently eligible (quota, cooldown or concurrency); retry later"}
			}
			return nil, lastErr
		}
		tried[member.id] = true
		member.markActive(1)
		result, err := member.client.GenerateVideo(ctx, prompt, model, aspect, files, 0)
		member.markActive(-1)
		if err == nil {
			return result, nil
		}
		lastErr = err
		var videoErr *VideoError
		if errors.As(err, &videoErr) && videoErr.Code == VideoErrorQuota {
			member.setQuotaUntil(time.Now().Add(time.Duration(p.sched.QuotaExclusionHours) * time.Hour))
			p.log.Warn("Pool account hit its video quota; excluded", zap.String("account", member.id), zap.Int("hours", p.sched.QuotaExclusionHours))
			continue
		}
		if isBotProtectionReply(err) {
			member.setBotUntil(time.Now().Add(time.Duration(p.sched.BotProtectionCooldownSeconds) * time.Second))
			p.log.Warn("Pool account replied with bot protection; cooling down and switching",
				zap.String("account", member.id),
				zap.Int("cooldown_seconds", p.sched.BotProtectionCooldownSeconds))
			if waitErr := sleepContext(ctx, time.Duration(p.sched.BotProtectionCooldownSeconds)*time.Second); waitErr != nil {
				return nil, &VideoError{Code: VideoErrorTimeout, Message: "Job canceled during bot-protection cooldown"}
			}
			continue
		}
		// Upstream/transient errors also move to the next account.
		continue
	}
	return nil, lastErr
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status reports pool state for operations.
type PoolStatus struct {
	Accounts []PoolAccountStatus `json:"accounts"`
}

type PoolAccountStatus struct {
	ID          string    `json:"id"`
	Healthy     bool      `json:"healthy"`
	Active      int       `json:"active"`
	QuotaUntil  time.Time `json:"quota_until,omitempty"`
	BotUntil    time.Time `json:"bot_until,omitempty"`
}

func (p *ClientPool) Status() PoolStatus {
	now := time.Now()
	status := PoolStatus{}
	for _, member := range p.members {
		member.mu.Lock()
		status.Accounts = append(status.Accounts, PoolAccountStatus{
			ID: member.id, Healthy: member.healthy, Active: member.active,
			QuotaUntil: member.quotaUntil, BotUntil: member.botUntil,
		})
		member.mu.Unlock()
		_ = now
	}
	return status
}

// resolveProxyTransport returns an http.Transport that dials through the
// given proxy. Without a proxy the default transport is returned untouched,
// preserving test injections of custom RoundTrippers.
func resolveProxyTransport(proxyURL string) http.RoundTripper {
	if strings.TrimSpace(proxyURL) == "" {
		return http.DefaultTransport
	}
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return transport
	}
	cloned := transport.Clone()
	cloned.Proxy = http.ProxyURL(parsed)
	return cloned
}

// plainHTTPClient builds a proxy-aware client for the ad-hoc net/http call
// sites (conversation reads, media downloads) that do not use req.
func (c *Client) plainHTTPClient(proxyURL string, timeout time.Duration) *http.Client {
	c.mu.RLock()
	resolved := proxyURL
	if resolved == "" {
		resolved = c.proxyURL
	}
	c.mu.RUnlock()
	return &http.Client{Timeout: timeout, Transport: resolveProxyTransport(resolved)}
}
