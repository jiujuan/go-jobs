package jobtpl

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 辅助 ─────────────────────────────────────────────────────────────────────

func validTemplate(name string) *JobTemplate {
	return &JobTemplate{
		Name:           name,
		Description:    "test template",
		ExecutorApp:    "test-app",
		ExecuteType:    model.ExecuteTypeBean,
		ExecuteHandler: "testHandler",
		ExecuteParam:   `{"key":"{{.Date}}"}`,
		RouteStrategy:  model.RouteRoundRobin,
		BlockStrategy:  model.BlockDiscard,
		MisfireStrategy: model.MisfireIgnore,
		JobType:        model.JobTypeCron,
		CronExpression: "0 * * * * *",
		Timeout:        60,
		RetryCount:     3,
		RetryInterval:  10,
		ShardingNum:    1,
		Labels:         map[string]string{"team": "data", "env": "prod"},
		CreateUser:     "admin",
	}
}

func validOverride(name string) *Override {
	return &Override{
		JobName:    name,
		ExecutorID: 1,
	}
}

// ─── JobTemplate.Validate ────────────────────────────────────────────────────

func TestJobTemplate_Validate_Valid(t *testing.T) {
	assert.NoError(t, validTemplate("t1").Validate())
}

func TestJobTemplate_Validate_MissingName(t *testing.T) {
	tpl := validTemplate("")
	assert.ErrorIs(t, tpl.Validate(), ErrInvalidTemplate)
}

func TestJobTemplate_Validate_MissingExecutorApp(t *testing.T) {
	tpl := validTemplate("t1")
	tpl.ExecutorApp = ""
	assert.ErrorIs(t, tpl.Validate(), ErrInvalidTemplate)
}

func TestJobTemplate_Validate_MissingHandler(t *testing.T) {
	tpl := validTemplate("t1")
	tpl.ExecuteHandler = ""
	assert.ErrorIs(t, tpl.Validate(), ErrInvalidTemplate)
}

// ─── JobTemplate.Clone ────────────────────────────────────────────────────────

func TestJobTemplate_Clone_DeepCopy(t *testing.T) {
	tpl := validTemplate("t1")
	cp := tpl.Clone()
	cp.Labels["team"] = "infra"
	assert.Equal(t, "data", tpl.Labels["team"], "修改拷贝不应影响原始对象")
}

func TestJobTemplate_Clone_NilLabels(t *testing.T) {
	tpl := validTemplate("t1")
	tpl.Labels = nil
	cp := tpl.Clone()
	assert.Nil(t, cp.Labels)
}

// ─── Override.Validate ───────────────────────────────────────────────────────

func TestOverride_Validate_Valid(t *testing.T) {
	assert.NoError(t, validOverride("job1").Validate())
}

func TestOverride_Validate_MissingJobName(t *testing.T) {
	ov := validOverride("")
	assert.ErrorIs(t, ov.Validate(), ErrInvalidTemplate)
}

func TestOverride_Validate_ZeroExecutorID(t *testing.T) {
	ov := &Override{JobName: "j", ExecutorID: 0}
	assert.ErrorIs(t, ov.Validate(), ErrInvalidTemplate)
}

// ─── Instantiate ─────────────────────────────────────────────────────────────

func TestInstantiate_InheritsTemplateFields(t *testing.T) {
	tpl := validTemplate("t1")
	ov := validOverride("my-job")

	job, err := Instantiate(tpl, ov)
	require.NoError(t, err)

	assert.Equal(t, "my-job", job.JobName)
	assert.Equal(t, int64(1), job.ExecutorID)
	assert.Equal(t, tpl.ExecutorApp, job.ExecutorApp)
	assert.Equal(t, tpl.ExecuteHandler, job.ExecuteHandler)
	assert.Equal(t, tpl.ExecuteParam, job.ExecuteParam)
	assert.Equal(t, tpl.RouteStrategy, job.RouteStrategy)
	assert.Equal(t, tpl.BlockStrategy, job.BlockStrategy)
	assert.Equal(t, tpl.Timeout, job.Timeout)
	assert.Equal(t, tpl.RetryCount, job.RetryCount)
	assert.Equal(t, tpl.CronExpression, job.CronExpression)
}

func TestInstantiate_OverrideFields(t *testing.T) {
	tpl := validTemplate("t1")
	ov := &Override{
		JobName:        "overridden",
		ExecutorID:     99,
		ExecutorApp:    "new-app",
		ExecuteHandler: "newHandler",
		ExecuteParam:   `{"override":true}`,
		Timeout:        120,
		RetryCount:     5,
		ShardingNum:    4,
	}

	job, err := Instantiate(tpl, ov)
	require.NoError(t, err)

	assert.Equal(t, "new-app", job.ExecutorApp)
	assert.Equal(t, "newHandler", job.ExecuteHandler)
	assert.Equal(t, `{"override":true}`, job.ExecuteParam)
	assert.Equal(t, 120, job.Timeout)
	assert.Equal(t, 5, job.RetryCount)
	assert.Equal(t, 4, job.ShardingNum)
}

func TestInstantiate_StatusDefaultsToStop(t *testing.T) {
	job, err := Instantiate(validTemplate("t1"), validOverride("j"))
	require.NoError(t, err)
	assert.Equal(t, model.JobStop, job.Status, "新实例默认应为停止状态")
}

func TestInstantiate_ShardingNumMinOne(t *testing.T) {
	tpl := validTemplate("t1")
	tpl.ShardingNum = 0
	job, err := Instantiate(tpl, validOverride("j"))
	require.NoError(t, err)
	assert.Equal(t, 1, job.ShardingNum, "ShardingNum 最小值为 1")
}

func TestInstantiate_NilTemplate_ReturnsError(t *testing.T) {
	_, err := Instantiate(nil, validOverride("j"))
	assert.ErrorIs(t, err, ErrInvalidTemplate)
}

func TestInstantiate_InvalidOverride_ReturnsError(t *testing.T) {
	_, err := Instantiate(validTemplate("t1"), &Override{})
	assert.ErrorIs(t, err, ErrInvalidTemplate)
}

func TestInstantiate_RouteStrategyOverride(t *testing.T) {
	tpl := validTemplate("t1")
	ov := &Override{
		JobName:       "j",
		ExecutorID:    1,
		RouteStrategy: model.RouteFailover,
	}
	job, err := Instantiate(tpl, ov)
	require.NoError(t, err)
	assert.Equal(t, model.RouteFailover, job.RouteStrategy)
}

func TestInstantiate_CronExpressionOverride(t *testing.T) {
	tpl := validTemplate("t1")
	ov := &Override{
		JobName:        "j",
		ExecutorID:     1,
		CronExpression: "0 0 * * * *",
	}
	job, err := Instantiate(tpl, ov)
	require.NoError(t, err)
	assert.Equal(t, "0 0 * * * *", job.CronExpression)
}

func TestInstantiate_DescriptionFromJobDesc(t *testing.T) {
	tpl := validTemplate("t1")
	ov := &Override{
		JobName:    "j",
		ExecutorID: 1,
		JobDesc:    "custom description",
	}
	job, err := Instantiate(tpl, ov)
	require.NoError(t, err)
	assert.Equal(t, "custom description", job.JobDesc)
}

func TestInstantiate_DescriptionFallsBackToTemplateDesc(t *testing.T) {
	tpl := validTemplate("t1")
	tpl.Description = "template desc"
	job, err := Instantiate(tpl, validOverride("j"))
	require.NoError(t, err)
	assert.Equal(t, "template desc", job.JobDesc)
}

// ─── Registry.Create ─────────────────────────────────────────────────────────

func TestRegistry_Create_Success(t *testing.T) {
	reg := NewRegistry()
	tpl, err := reg.Create(validTemplate("t1"))
	require.NoError(t, err)
	assert.Greater(t, tpl.ID, int64(0))
	assert.False(t, tpl.CreateTime.IsZero())
	assert.Equal(t, 1, reg.Len())
}

func TestRegistry_Create_AssignsUniqueIDs(t *testing.T) {
	reg := NewRegistry()
	t1, _ := reg.Create(validTemplate("t1"))
	t2, _ := reg.Create(validTemplate("t2"))
	assert.NotEqual(t, t1.ID, t2.ID)
}

func TestRegistry_Create_DuplicateName_Errors(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	_, err := reg.Create(validTemplate("t1"))
	assert.ErrorIs(t, err, ErrTemplateExists)
}

func TestRegistry_Create_InvalidTemplate_Errors(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Create(&JobTemplate{Name: "bad"}) // missing executor_app
	assert.ErrorIs(t, err, ErrInvalidTemplate)
}

func TestRegistry_Create_ReturnsCopy(t *testing.T) {
	reg := NewRegistry()
	original := validTemplate("t1")
	returned, _ := reg.Create(original)
	returned.ExecutorApp = "modified"
	stored, _ := reg.Get("t1")
	assert.Equal(t, "test-app", stored.ExecutorApp, "返回值是副本，修改不应影响注册表")
}

// ─── Registry.Update ─────────────────────────────────────────────────────────

func TestRegistry_Update_Success(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	updated := validTemplate("t1")
	updated.Timeout = 300
	result, err := reg.Update(updated)
	require.NoError(t, err)
	assert.Equal(t, 300, result.Timeout)
}

func TestRegistry_Update_PreservesIDAndCreateTime(t *testing.T) {
	reg := NewRegistry()
	created, _ := reg.Create(validTemplate("t1"))
	time.Sleep(1 * time.Millisecond)
	updated := validTemplate("t1")
	result, err := reg.Update(updated)
	require.NoError(t, err)
	assert.Equal(t, created.ID, result.ID)
	assert.Equal(t, created.CreateTime, result.CreateTime)
	assert.True(t, result.UpdateTime.After(created.UpdateTime))
}

func TestRegistry_Update_NotFound_Errors(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Update(validTemplate("nonexistent"))
	assert.ErrorIs(t, err, ErrTemplateNotFound)
}

// ─── Registry.Delete ─────────────────────────────────────────────────────────

func TestRegistry_Delete_Success(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	require.NoError(t, reg.Delete("t1"))
	assert.Equal(t, 0, reg.Len())
}

func TestRegistry_Delete_NotFound_Errors(t *testing.T) {
	reg := NewRegistry()
	assert.ErrorIs(t, reg.Delete("missing"), ErrTemplateNotFound)
}

func TestRegistry_Delete_RemovesIDIndex(t *testing.T) {
	reg := NewRegistry()
	tpl, _ := reg.Create(validTemplate("t1"))
	reg.Delete("t1")
	_, err := reg.GetByID(tpl.ID)
	assert.ErrorIs(t, err, ErrTemplateNotFound)
}

// ─── Registry.Get / GetByID ──────────────────────────────────────────────────

func TestRegistry_Get_Success(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	tpl, err := reg.Get("t1")
	require.NoError(t, err)
	assert.Equal(t, "t1", tpl.Name)
}

func TestRegistry_Get_NotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Get("missing")
	assert.ErrorIs(t, err, ErrTemplateNotFound)
}

func TestRegistry_GetByID_Success(t *testing.T) {
	reg := NewRegistry()
	created, _ := reg.Create(validTemplate("t1"))
	tpl, err := reg.GetByID(created.ID)
	require.NoError(t, err)
	assert.Equal(t, "t1", tpl.Name)
}

func TestRegistry_GetByID_NotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.GetByID(9999)
	assert.ErrorIs(t, err, ErrTemplateNotFound)
}

// ─── Registry.List ───────────────────────────────────────────────────────────

func TestRegistry_List_All(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	reg.Create(validTemplate("t2"))
	reg.Create(validTemplate("t3"))
	list := reg.List(nil)
	assert.Len(t, list, 3)
}

func TestRegistry_List_WithFilter(t *testing.T) {
	reg := NewRegistry()
	tpl1 := validTemplate("t1")
	tpl1.Labels = map[string]string{"env": "prod"}
	tpl2 := validTemplate("t2")
	tpl2.Labels = map[string]string{"env": "dev"}
	reg.Create(tpl1)
	reg.Create(tpl2)

	prodList := reg.List(func(t *JobTemplate) bool {
		return t.Labels["env"] == "prod"
	})
	assert.Len(t, prodList, 1)
	assert.Equal(t, "t1", prodList[0].Name)
}

func TestRegistry_List_Empty(t *testing.T) {
	reg := NewRegistry()
	assert.Empty(t, reg.List(nil))
}

// ─── Registry.Instantiate ────────────────────────────────────────────────────

func TestRegistry_Instantiate_Success(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	job, err := reg.Instantiate("t1", validOverride("my-job"))
	require.NoError(t, err)
	assert.Equal(t, "my-job", job.JobName)
	assert.Equal(t, "test-app", job.ExecutorApp)
}

func TestRegistry_Instantiate_TemplateNotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Instantiate("missing", validOverride("j"))
	assert.ErrorIs(t, err, ErrTemplateNotFound)
}

func TestRegistry_InstantiateByID_Success(t *testing.T) {
	reg := NewRegistry()
	tpl, _ := reg.Create(validTemplate("t1"))
	job, err := reg.InstantiateByID(tpl.ID, validOverride("j"))
	require.NoError(t, err)
	assert.Equal(t, "j", job.JobName)
}

// ─── Registry.BatchInstantiate ───────────────────────────────────────────────

func TestRegistry_BatchInstantiate_AllSuccess(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	overrides := []*Override{
		{JobName: "job-1", ExecutorID: 1},
		{JobName: "job-2", ExecutorID: 1, ShardingNum: 4},
		{JobName: "job-3", ExecutorID: 2, ExecutorApp: "other-app"},
	}
	jobs, err := reg.BatchInstantiate("t1", overrides)
	require.NoError(t, err)
	assert.Len(t, jobs, 3)
	assert.Equal(t, "job-1", jobs[0].JobName)
	assert.Equal(t, 4, jobs[1].ShardingNum)
	assert.Equal(t, "other-app", jobs[2].ExecutorApp)
}

func TestRegistry_BatchInstantiate_PartialFailure(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))
	overrides := []*Override{
		{JobName: "ok-job", ExecutorID: 1},
		{JobName: "", ExecutorID: 1}, // invalid
	}
	jobs, err := reg.BatchInstantiate("t1", overrides)
	assert.Error(t, err, "部分失败应返回错误")
	assert.Len(t, jobs, 1, "已成功的结果应返回")
}

func TestRegistry_BatchInstantiate_TemplateNotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.BatchInstantiate("missing", []*Override{validOverride("j")})
	assert.ErrorIs(t, err, ErrTemplateNotFound)
}

// ─── Export / Import ─────────────────────────────────────────────────────────

func TestRegistry_ExportAndImport_Roundtrip(t *testing.T) {
	reg1 := NewRegistry()
	reg1.Create(validTemplate("t1"))
	reg1.Create(validTemplate("t2"))

	exported := reg1.Export()
	assert.Len(t, exported, 2)

	reg2 := NewRegistry()
	err := reg2.Import(exported)
	require.NoError(t, err)
	assert.Equal(t, 2, reg2.Len())

	tpl, err := reg2.Get("t1")
	require.NoError(t, err)
	assert.Equal(t, "test-app", tpl.ExecutorApp)
}

func TestRegistry_Import_IdempotentUpdate(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("t1"))

	updated := validTemplate("t1")
	updated.Timeout = 999
	err := reg.Import([]*JobTemplate{updated})
	require.NoError(t, err)

	tpl, _ := reg.Get("t1")
	assert.Equal(t, 999, tpl.Timeout)
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestRegistry_ConcurrentCreateAndGet(t *testing.T) {
	reg := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			reg.Create(validTemplate(fmt.Sprintf("t%d", i)))
		}(i)
		go func(i int) {
			defer wg.Done()
			reg.Get(fmt.Sprintf("t%d", i))
		}(i)
	}
	wg.Wait()
}

func TestRegistry_ConcurrentInstantiate(t *testing.T) {
	reg := NewRegistry()
	reg.Create(validTemplate("shared"))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := reg.Instantiate("shared", &Override{
				JobName:    fmt.Sprintf("job-%d", i),
				ExecutorID: int64(i + 1),
			})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

// ─── coalesce helpers ─────────────────────────────────────────────────────────

func TestCoalesceStr_OverrideTakesPrecedence(t *testing.T) {
	assert.Equal(t, "override", coalesceStr("override", "fallback"))
}

func TestCoalesceStr_FallbackWhenOverrideEmpty(t *testing.T) {
	assert.Equal(t, "fallback", coalesceStr("", "fallback"))
}

func TestCoalesceInt_OverrideTakesPrecedence(t *testing.T) {
	assert.Equal(t, 5, coalesceInt(5, 10))
}

func TestCoalesceInt_FallbackWhenZero(t *testing.T) {
	assert.Equal(t, 10, coalesceInt(0, 10))
}
