package providers

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestPoolSchedulerConfigDefaults(t *testing.T) {
	config := &PoolSchedulerConfig{MaxConcurrentPerAccount: 9, MaxConcurrentPerAccountCap: 4}
	config.fillDefaults()
	if config.Selection != "random" {
		t.Fatalf("selection = %q, want random", config.Selection)
	}
	if config.MaxConcurrentPerAccount != 4 {
		t.Fatalf("per-account concurrency = %d, want capped at 4", config.MaxConcurrentPerAccount)
	}
	if config.BotProtectionCooldownSeconds != 180 {
		t.Fatalf("bot cooldown = %d, want 180", config.BotProtectionCooldownSeconds)
	}
	if config.QuotaExclusionHours != 6 {
		t.Fatalf("quota exclusion = %d hours, want 6", config.QuotaExclusionHours)
	}
	if config.SubmitIntervalSecondsMax < config.SubmitIntervalSecondsMin {
		t.Fatalf("submit interval max %d below min %d", config.SubmitIntervalSecondsMax, config.SubmitIntervalSecondsMin)
	}
}

func TestLoadPoolConfigValidation(t *testing.T) {
	log := zap.NewNop()
	if config := LoadPoolConfig("", log); config != nil {
		t.Fatal("empty path must disable the pool")
	}

	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	if config := LoadPoolConfig(write("broken.json", "{"), log); config != nil {
		t.Fatal("invalid JSON must disable the pool")
	}
	if config := LoadPoolConfig(write("empty.json", `{"accounts":[]}`), log); config != nil {
		t.Fatal("no accounts must disable the pool")
	}

	dup := `{"accounts":[
		{"id":"a","cookies":"__Secure-1PSID=x; __Secure-1PSIDTS=y"},
		{"id":"a","cookies":"__Secure-1PSID=x2; __Secure-1PSIDTS=y2"},
		{"id":"b","cookies":""}
	]}`
	config := LoadPoolConfig(write("dup.json", dup), log)
	if config == nil || len(config.Accounts) != 1 || config.Accounts[0].ID != "a" {
		t.Fatalf("duplicate/empty accounts not filtered: %+v", config)
	}
}

func newTestPoolMember(id string, generate func(ctx context.Context, prompt, model string, aspect VideoAspectRatio, files []InputFile) (*VideoResult, error)) *poolMember {
	client := &Client{log: zap.NewNop(), generateVideoFn: generate}
	return &poolMember{id: id, client: client, healthy: true}
}

func TestPoolPickExcludesUnavailable(t *testing.T) {
	now := time.Now()
	fake := func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
		return &VideoResult{Video: Video{MimeType: "video/mp4"}, Data: []byte("mp4")}, nil
	}
	pool := &ClientPool{
		sched: PoolSchedulerConfig{Selection: "first", MaxConcurrentPerAccount: 1, MaxConcurrentPerAccountCap: 5},
		log:   zap.NewNop(),
		members: []*poolMember{
			newTestPoolMember("quota", fake),
			newTestPoolMember("bot", fake),
			newTestPoolMember("down", fake),
			newTestPoolMember("busy", fake),
			newTestPoolMember("ok", fake),
		},
	}
	pool.members[0].setQuotaUntil(now.Add(time.Hour))
	pool.members[1].setBotUntil(now.Add(time.Hour))
	pool.members[2].setHealthy(false)
	pool.members[3].markActive(1)

	picked := pool.pick(now, nil)
	if picked == nil || picked.id != "ok" {
		t.Fatalf("pick = %+v, want the only eligible member ok", picked)
	}
	if pool.pick(now, map[string]bool{"ok": true}) != nil {
		t.Fatal("skip map must exclude the last eligible member")
	}
}

func TestPoolGenerateVideoSwitchesOnQuota(t *testing.T) {
	calls := map[string]int{}
	pool := &ClientPool{
		sched: PoolSchedulerConfig{Selection: "first", MaxConcurrentPerAccount: 3, MaxConcurrentPerAccountCap: 5, QuotaExclusionHours: 6},
		log:   zap.NewNop(),
		members: []*poolMember{
			newTestPoolMember("a", func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
				calls["a"]++
				return nil, &VideoError{Code: VideoErrorQuota, Message: "limit reached"}
			}),
			newTestPoolMember("b", func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
				calls["b"]++
				return &VideoResult{Video: Video{MimeType: "video/mp4"}, Data: []byte("mp4")}, nil
			}),
		},
	}
	result, err := pool.generateVideoOnPool(context.Background(), "A cat", "", VideoAspectLandscape, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Data) != "mp4" || calls["a"] != 1 || calls["b"] != 1 {
		t.Fatalf("quota did not switch accounts: calls=%v result=%v", calls, result)
	}
	if !pool.members[0].quotaUntil.After(time.Now()) {
		t.Fatal("quota account was not excluded")
	}
}

func TestPoolGenerateVideoBotCooldownThenSwitch(t *testing.T) {
	calls := map[string]int{}
	pool := &ClientPool{
		sched: PoolSchedulerConfig{Selection: "first", MaxConcurrentPerAccount: 3, MaxConcurrentPerAccountCap: 5, BotProtectionCooldownSeconds: 1},
		log:   zap.NewNop(),
		members: []*poolMember{
			newTestPoolMember("a", func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
				calls["a"]++
				return nil, errors.New("Google Gemini Web returned an error (BardErrorInfo code [13 1152])")
			}),
			newTestPoolMember("b", func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
				calls["b"]++
				return &VideoResult{Video: Video{MimeType: "video/mp4"}, Data: []byte("mp4")}, nil
			}),
		},
	}
	before := time.Now()
	if _, err := pool.generateVideoOnPool(context.Background(), "A cat", "", VideoAspectLandscape, nil); err != nil {
		t.Fatal(err)
	}
	if calls["a"] != 1 || calls["b"] != 1 {
		t.Fatalf("bot protection did not switch accounts: calls=%v", calls)
	}
	if !pool.members[0].botUntil.After(before) {
		t.Fatal("bot-protected account was not cooled down")
	}
}

func TestPoolGenerateVideoAllExcluded(t *testing.T) {
	pool := &ClientPool{
		sched: PoolSchedulerConfig{Selection: "first", MaxConcurrentPerAccount: 3, MaxConcurrentPerAccountCap: 5, QuotaExclusionHours: 6},
		log:   zap.NewNop(),
		members: []*poolMember{
			newTestPoolMember("a", func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
				return nil, &VideoError{Code: VideoErrorQuota, Message: "limit"}
			}),
			newTestPoolMember("b", func(context.Context, string, string, VideoAspectRatio, []InputFile) (*VideoResult, error) {
				return nil, &VideoError{Code: VideoErrorQuota, Message: "limit"}
			}),
		},
	}
	_, err := pool.generateVideoOnPool(context.Background(), "A cat", "", VideoAspectLandscape, nil)
	var videoErr *VideoError
	if !errors.As(err, &videoErr) || videoErr.Code != VideoErrorQuota {
		t.Fatalf("all-quota pool error = %v, want VideoErrorQuota", err)
	}
}

func TestVideoStoreMaxActive(t *testing.T) {
	release := make(chan struct{})
	store := newVideoJobStore(func(ctx context.Context, prompt, model string, aspect VideoAspectRatio, files []InputFile) (*VideoResult, error) {
		<-release
		return &VideoResult{Video: Video{MimeType: "video/mp4"}, Data: []byte("mp4")}, nil
	}, time.Minute)
	store.setMaxActive(2)

	first, err := store.start("one", "m", VideoAspectLandscape, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.start("two", "m", VideoAspectLandscape, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.start("three", "m", VideoAspectLandscape, nil); !errors.Is(err, ErrVideoBusy) {
		t.Fatalf("third concurrent job error = %v, want ErrVideoBusy", err)
	}
	close(release)
	waitForStoreJob(t, store, first.ID)
	waitForStoreJob(t, store, second.ID)
	store.close()
}

func waitForStoreJob(t *testing.T, store *videoJobStore, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := store.get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == VideoJobCompleted {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("store job %s never completed", id)
}
