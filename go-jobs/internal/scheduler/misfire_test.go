package scheduler

// misfire_test.go
//
// 补充覆盖 misfire.go 中 scheduler_test.go 未触达的边界分支：
//
// handleMisfire
//   scheduler_test.go 已覆盖：
//     · MisfireIgnore（overdue > threshold）
//     · MisfireRunOnce（overdue > threshold）
//     · 阈值内（overdue ≤ threshold）→ false
//
//   本文件补充：
//     1.  NextTriggerTime == nil → 立即返回 false（不进入任何 switch 分支）
//     2.  MisfireStrategy = default（未知值）→ 超过阈值也返回 false
//     3.  MisfireRunOnce + workerCh 已满 → 不阻塞，直接丢弃，reschedule 仍执行
//     4.  MisfireIgnore → reschedule 后 NextTriggerTime 推进到未来
//     5.  MisfireRunOnce → reschedule 后 NextTriggerTime 推进到未来
//     6.  刚好等于阈值（overdue == misfireThreshold）→ 不算 misfire（≤ 而非 <）
//     7.  overdue 极大（模拟长时间宕机）→ 仍正常处理
//     8.  MisfireIgnore + 无 cron → reschedule 把任务置为 JobStop

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// mfScheduler 构建一个最小 Scheduler 用于 misfire 测试。
// 不启动 Start()，只借用 handleMisfire / reschedule 方法。
func mfScheduler() *Scheduler {
	return New(
		newMockJobDAO(),
		newMockLogDAO(),
		newMockExecutorDAO("", ""),
		nil, // redis nil
		nil, // executorStore nil
		WithNodeID("mf-test"),
	)
}

// mfJob 构建一个带 cron 表达式且 Status=JobRun 的任务。
// nextTriggerOffset 是相对 now 的偏移：负值表示已过期。
func mfJob(id int64, cronExpr string, nextTriggerOffset time.Duration, strategy model.MisfireStrategy) *model.JobInfo {
	j := &model.JobInfo{
		ID:              id,
		ExecutorApp:     "test-app",
		JobName:         "mf-job",
		CronExpression:  cronExpr,
		Status:          model.JobRun,
		MisfireStrategy: strategy,
	}
	t := time.Now().Add(nextTriggerOffset)
	j.NextTriggerTime = &t
	return j
}

// ─── 1. NextTriggerTime == nil ───────────────────────────────────────────────

func TestHandleMisfire_NilNextTriggerTime_ReturnsFalse(t *testing.T) {
	s := mfScheduler()
	job := &model.JobInfo{
		ID:              1,
		MisfireStrategy: model.MisfireIgnore,
		// NextTriggerTime 故意不设置（nil）
	}
	result := s.handleMisfire(context.Background(), job)
	assert.False(t, result, "NextTriggerTime 为 nil 时应直接返回 false")
}

// ─── 2. 未知 MisfireStrategy → 超过阈值也返回 false ─────────────────────────

func TestHandleMisfire_UnknownStrategy_OverThreshold_ReturnsFalse(t *testing.T) {
	s := mfScheduler()
	// overdue = 30s >> misfireThreshold(5s)，但策略未知 → default 分支 → false
	job := mfJob(2, "* * * * * ?", -30*time.Second, model.MisfireStrategy(99))
	result := s.handleMisfire(context.Background(), job)
	assert.False(t, result, "未知 MisfireStrategy 超过阈值仍应返回 false")
}

// ─── 3. MisfireRunOnce + workerCh 已满 → 不阻塞 ────────────────────────────

func TestHandleMisfire_RunOnce_WorkerChannelFull_DoesNotBlock(t *testing.T) {
	s := mfScheduler()
	// 把 workerCh 换成容量为 0 的 channel，模拟"已满"（无缓冲）
	s.workerCh = make(chan *triggerTask, 0)

	// 需要 store 里有该任务，否则 reschedule 更新会找不到
	job := mfJob(3, "* * * * * ?", -10*time.Second, model.MisfireRunOnce)
	s.store.Add(job)

	done := make(chan bool, 1)
	go func() {
		result := s.handleMisfire(context.Background(), job)
		done <- result
	}()

	select {
	case result := <-done:
		assert.True(t, result, "workerCh 满时应进入 default 丢弃分支，仍返回 true")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handleMisfire 在 workerCh 满时不应阻塞")
	}
}

// ─── 4. MisfireIgnore → reschedule 推进 NextTriggerTime ──────────────────────

func TestHandleMisfire_Ignore_ReschedulesNextTrigger(t *testing.T) {
	s := mfScheduler()

	job := mfJob(4, "* * * * * ?", -10*time.Second, model.MisfireIgnore)
	s.store.Add(job)

	handled := s.handleMisfire(context.Background(), job)
	require.True(t, handled)

	fresh, ok := s.store.Get(4)
	require.True(t, ok)
	require.NotNil(t, fresh.NextTriggerTime)
	assert.True(t, fresh.NextTriggerTime.After(time.Now()),
		"MisfireIgnore 后 NextTriggerTime 应推进到未来")
}

// ─── 5. MisfireRunOnce → reschedule 推进 NextTriggerTime ────────────────────

func TestHandleMisfire_RunOnce_ReschedulesNextTrigger(t *testing.T) {
	s := mfScheduler()
	// workerCh 需要有空间，否则补偿投递失败（进 default 分支但仍 reschedule）
	s.workerCh = make(chan *triggerTask, 16)

	job := mfJob(5, "* * * * * ?", -10*time.Second, model.MisfireRunOnce)
	s.store.Add(job)

	handled := s.handleMisfire(context.Background(), job)
	require.True(t, handled)

	fresh, ok := s.store.Get(5)
	require.True(t, ok)
	require.NotNil(t, fresh.NextTriggerTime)
	assert.True(t, fresh.NextTriggerTime.After(time.Now()),
		"MisfireRunOnce 后 NextTriggerTime 应推进到未来")
}

// ─── 6. 刚好等于阈值（overdue == misfireThreshold）→ false ──────────────────

func TestHandleMisfire_ExactlyAtThreshold_ReturnsFalse(t *testing.T) {
	s := mfScheduler()

	// overdue = misfireThreshold → 满足 overdue <= misfireThreshold → 不算 misfire
	past := time.Now().Add(-misfireThreshold)
	job := &model.JobInfo{
		ID:              6,
		CronExpression:  "* * * * * ?",
		Status:          model.JobRun,
		MisfireStrategy: model.MisfireIgnore,
		NextTriggerTime: &past,
	}
	result := s.handleMisfire(context.Background(), job)
	assert.False(t, result, "overdue == misfireThreshold 不应视为 misfire")
}

// ─── 7. overdue 极大（模拟长时间宕机）→ 正常处理 ─────────────────────────────

func TestHandleMisfire_LargeOverdue_HandledCorrectly(t *testing.T) {
	s := mfScheduler()

	// overdue = 24h，远超阈值
	job := mfJob(7, "* * * * * ?", -24*time.Hour, model.MisfireIgnore)
	s.store.Add(job)

	result := s.handleMisfire(context.Background(), job)
	assert.True(t, result, "极大 overdue 应被正常识别为 misfire 并处理")
}

// ─── 8. MisfireIgnore + 无 cron → reschedule 置 JobStop ─────────────────────

func TestHandleMisfire_Ignore_NoCronExpr_RescheduleSetsJobStop(t *testing.T) {
	s := mfScheduler()

	// CronExpression 为空 → reschedule 将 status 置为 JobStop
	past := time.Now().Add(-10 * time.Second)
	job := &model.JobInfo{
		ID:              8,
		CronExpression:  "", // 一次性任务
		Status:          model.JobRun,
		MisfireStrategy: model.MisfireIgnore,
		NextTriggerTime: &past,
	}
	s.store.Add(job)

	handled := s.handleMisfire(context.Background(), job)
	require.True(t, handled)

	fresh, ok := s.store.Get(8)
	require.True(t, ok)
	assert.Equal(t, model.JobStop, fresh.Status,
		"一次性任务 Misfire 后 reschedule 应将其置为 JobStop")
}

// ─── misfireThreshold 常量值验证 ─────────────────────────────────────────────

func TestMisfireThreshold_Is5Seconds(t *testing.T) {
	assert.Equal(t, 5*time.Second, misfireThreshold,
		"misfireThreshold 应为 5s")
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestHandleMisfire_ConcurrentCalls_NoRace(t *testing.T) {
	s := mfScheduler()
	s.workerCh = make(chan *triggerTask, 256)

	job := mfJob(9, "* * * * * ?", -10*time.Second, model.MisfireIgnore)
	s.store.Add(job)

	done := make(chan struct{})
	for i := 0; i < 30; i++ {
		go func() {
			// 每次传入独立副本，避免并发修改同一 job 指针
			j := mfJob(9, "* * * * * ?", -10*time.Second, model.MisfireIgnore)
			s.handleMisfire(context.Background(), j)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 30; i++ {
		<-done
	}
}
