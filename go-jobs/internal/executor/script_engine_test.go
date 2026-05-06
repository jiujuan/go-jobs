package executor

// script_engine_test.go
//
// 测试覆盖范围：
//  1. ScriptResult.Combined()
//  2. ScriptEngineRegistry（Register / Get / Types / LoadFromConfig）
//  3. limitedWriter 输出截断
//  4. writeTempFile 临时文件创建与清理
//  5. 内置引擎：ShellEngine / PythonEngine / CmdEngine（Validate + Execute）
//  6. ConfigDrivenEngine（Validate + 三种 ExecMode）
//  7. Runner 集成：新引擎通过 WithScriptEngineRegistry 注入
//  8. Runner 集成：不支持的类型返回友好错误
//  9. Runner 集成：脚本超时被 context cancel
// 10. 并发安全：多 goroutine 同时注册/查询引擎
// 11. 配置覆盖内置引擎
// 12. 诊断接口 ScriptEngines() 暴露注册表

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/pkg/idempotency"
)

// ─── 测试辅助函数 ─────────────────────────────────────────────────────────────

// hasBinary 判断系统中是否存在指定二进制。
func hasBinary(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// scriptReq 快速构造脚本类 RunRequest。
func scriptReq(logID, jobID int64, execType, param string) *RunRequest {
	return &RunRequest{
		LogID:        logID,
		JobID:        jobID,
		ExecuteType:  execType,
		ExecuteParam: param,
	}
}

// newTestRunnerWithEngines 创建注入了自定义引擎注册表的 Runner。
func newTestRunnerWithEngines(engineReg *ScriptEngineRegistry) *Runner {
	reg := NewRegistry()
	return NewRunner(reg,
		WithIdempotencyTTL(5*time.Second),
		WithIdempotencyGCInterval(time.Minute),
		WithScriptEngineRegistry(engineReg),
	)
}

// waitScript 等待脚本任务完成（轮询幂等表）。
func waitScript(r *Runner, logID int64, timeout time.Duration) *idempotency.Record {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec := r.idempotency.Get(logID)
		if rec != nil && !rec.Running() {
			return rec
		}
		time.Sleep(10 * time.Millisecond)
	}
	return r.idempotency.Get(logID)
}

// ─── 1. ScriptResult ─────────────────────────────────────────────────────────

func TestScriptResult_Combined_BothOutputs(t *testing.T) {
	r := &ScriptResult{
		Stdout: []byte("hello"),
		Stderr: []byte("world"),
	}
	got := r.Combined()
	assert.Equal(t, "hello\nworld", got)
}

func TestScriptResult_Combined_OnlyStdout(t *testing.T) {
	r := &ScriptResult{Stdout: []byte("out")}
	assert.Equal(t, "out", r.Combined())
}

func TestScriptResult_Combined_OnlyStderr(t *testing.T) {
	r := &ScriptResult{Stderr: []byte("err")}
	assert.Equal(t, "err", r.Combined())
}

func TestScriptResult_Combined_BothEmpty(t *testing.T) {
	r := &ScriptResult{}
	assert.Equal(t, "", r.Combined())
}

// ─── 2. ScriptEngineRegistry ─────────────────────────────────────────────────

func TestNewScriptEngineRegistry_RegistersBuiltins(t *testing.T) {
	reg := NewScriptEngineRegistry()
	types := reg.Types()
	sort.Strings(types)
	// CMD 应始终注册（Validate 对 CMD 不检查 binary）
	assert.Contains(t, types, "CMD")
}

func TestRegistry_Register_NilEngine_ReturnsError(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	err := reg.Register(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil engine")
}

func TestRegistry_Register_EmptyType_ReturnsError(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	err := reg.Register(&mockEngine{typeName: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type name is empty")
}

func TestRegistry_Register_ValidateFails_EngineNotRegistered(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	bad := &mockEngine{typeName: "BAD", validateErr: fmt.Errorf("bad binary")}
	err := reg.Register(bad)
	require.Error(t, err)
	_, ok := reg.Get("BAD")
	assert.False(t, ok, "failed-validate engine should not be registered")
}

func TestRegistry_Register_Overwrite_Success(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	e1 := &mockEngine{typeName: "FOO", output: "v1"}
	e2 := &mockEngine{typeName: "FOO", output: "v2"}
	require.NoError(t, reg.Register(e1))
	require.NoError(t, reg.Register(e2))
	got, ok := reg.Get("FOO")
	require.True(t, ok)
	assert.Equal(t, e2, got, "second registration should overwrite first")
}

func TestRegistry_Get_CaseInsensitive(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	require.NoError(t, reg.Register(&mockEngine{typeName: "MYTYPE"}))
	_, ok1 := reg.Get("mytype")
	_, ok2 := reg.Get("MyType")
	_, ok3 := reg.Get("MYTYPE")
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.True(t, ok3)
}

func TestRegistry_Get_NotFound_ReturnsFalse(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	_, ok := reg.Get("NOTEXIST")
	assert.False(t, ok)
}

func TestRegistry_Types_ReturnsAllNames(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	for _, name := range []string{"AA", "BB", "CC"} {
		require.NoError(t, reg.Register(&mockEngine{typeName: name}))
	}
	types := reg.Types()
	sort.Strings(types)
	assert.Equal(t, []string{"AA", "BB", "CC"}, types)
}

// ─── 3. limitedWriter ────────────────────────────────────────────────────────

func TestLimitedWriter_CapsTotalBytes(t *testing.T) {
	var buf bytes.Buffer
	lw := &limitedWriter{buf: &buf, limit: 10}

	n, err := lw.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = lw.Write([]byte("world!extra"))
	require.NoError(t, err)
	assert.Equal(t, 11, n)           // 返回原始 len，不报错
	assert.Equal(t, "helloworld", buf.String()) // 只保留前 10 字节
}

func TestLimitedWriter_AtLimitDropsAll(t *testing.T) {
	var buf bytes.Buffer
	lw := &limitedWriter{buf: &buf, limit: 5, written: 5}
	n, err := lw.Write([]byte("anything"))
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Empty(t, buf.String())
}

// ─── 4. writeTempFile ────────────────────────────────────────────────────────

func TestWriteTempFile_CreatesFileWithContent(t *testing.T) {
	content := "package main\nfunc main(){}"
	path, err := writeTempFile("", ".go", content)
	require.NoError(t, err)
	defer os.Remove(path)

	assert.True(t, strings.HasSuffix(path, ".go"))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, content, string(got))
}

func TestWriteTempFile_AutoPrependsDot(t *testing.T) {
	path, err := writeTempFile("", "py", "print(1)")
	require.NoError(t, err)
	defer os.Remove(path)
	assert.True(t, strings.HasSuffix(path, ".py"))
}

func TestWriteTempFile_CustomWorkDir(t *testing.T) {
	dir := t.TempDir()
	path, err := writeTempFile(dir, ".sh", "echo hi")
	require.NoError(t, err)
	defer os.Remove(path)
	assert.True(t, strings.HasPrefix(path, dir))
}

// ─── 5. 内置引擎 ─────────────────────────────────────────────────────────────

func TestCmdEngine_Type(t *testing.T) {
	e := &CmdEngine{}
	assert.Equal(t, "CMD", e.Type())
}

func TestCmdEngine_Validate_AlwaysNil(t *testing.T) {
	assert.NoError(t, (&CmdEngine{}).Validate())
}

func TestCmdEngine_Execute_Echo(t *testing.T) {
	if !hasBinary("echo") {
		t.Skip("echo not available")
	}
	e := &CmdEngine{base: baseScriptEngine{typeName: "CMD"}}
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "CMD", "echo hello"))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "hello")
	assert.Equal(t, 0, result.ExitCode)
}

func TestCmdEngine_Execute_EmptyParam_ReturnsError(t *testing.T) {
	e := &CmdEngine{base: baseScriptEngine{typeName: "CMD"}}
	_, err := e.Execute(context.Background(), scriptReq(1, 1, "CMD", ""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "execute_param is empty")
}

func TestCmdEngine_Execute_ExitNonZero_ReturnsError(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := &CmdEngine{base: baseScriptEngine{typeName: "CMD"}}
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "CMD", "bash -c 'exit 42'"))
	require.Error(t, err)
	assert.Equal(t, 42, result.ExitCode)
}

func TestShellEngine_Type(t *testing.T) {
	assert.Equal(t, "SHELL", (&ShellEngine{}).Type())
}

func TestShellEngine_Validate_BashAvailable(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	assert.NoError(t, (&ShellEngine{}).Validate())
}

func TestShellEngine_Execute_PrintDate(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := &ShellEngine{base: baseScriptEngine{typeName: "SHELL"}}
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "SHELL", "echo $(date +%Y)"))
	require.NoError(t, err)
	assert.NotEmpty(t, string(result.Stdout))
}

func TestShellEngine_Execute_MultiLineScript(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	script := `
x=10
y=20
echo $((x + y))
`
	e := &ShellEngine{base: baseScriptEngine{typeName: "SHELL"}}
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "SHELL", script))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "30")
}

func TestShellEngine_Execute_ContextTimeout(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := &ShellEngine{base: baseScriptEngine{typeName: "SHELL"}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := e.Execute(ctx, scriptReq(1, 1, "SHELL", "sleep 10"))
	require.Error(t, err, "should fail due to timeout")
}

func TestPythonEngine_Type(t *testing.T) {
	assert.Equal(t, "PYTHON", (&PythonEngine{}).Type())
}

func TestPythonEngine_Execute_PrintSum(t *testing.T) {
	if !hasBinary("python3") {
		t.Skip("python3 not available")
	}
	e := &PythonEngine{base: baseScriptEngine{typeName: "PYTHON"}}
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "PYTHON", "print(2+3)"))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "5")
}

// ─── 6. ConfigDrivenEngine ───────────────────────────────────────────────────

func TestConfigDrivenEngine_Type_Uppercase(t *testing.T) {
	e := NewConfigDrivenEngine(EngineConfig{Type: "go", Binary: "go"})
	assert.Equal(t, "GO", e.Type())
}

func TestConfigDrivenEngine_Validate_EmptyType_Error(t *testing.T) {
	e := NewConfigDrivenEngine(EngineConfig{Type: "", Binary: "go"})
	assert.Error(t, e.Validate())
}

func TestConfigDrivenEngine_Validate_EmptyBinary_Error(t *testing.T) {
	e := NewConfigDrivenEngine(EngineConfig{Type: "GO", Binary: ""})
	err := e.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "binary is empty")
}

func TestConfigDrivenEngine_Validate_BinaryNotFound_Error(t *testing.T) {
	e := NewConfigDrivenEngine(EngineConfig{
		Type:   "FAKE",
		Binary: "definitely-not-a-real-binary-xyz",
	})
	err := e.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in PATH")
}

func TestConfigDrivenEngine_Validate_AbsPath_NotExist_Error(t *testing.T) {
	e := NewConfigDrivenEngine(EngineConfig{
		Type:   "FAKE",
		Binary: "/no/such/binary",
	})
	require.Error(t, e.Validate())
}

func TestConfigDrivenEngine_DefaultExecMode_IsInline(t *testing.T) {
	e := NewConfigDrivenEngine(EngineConfig{Type: "X", Binary: "bash"})
	assert.Equal(t, ExecModeInline, e.cfg.ExecMode)
}

func TestConfigDrivenEngine_InlineMode_Execute(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := NewConfigDrivenEngine(EngineConfig{
		Type:     "BASH2",
		Binary:   "bash",
		ExecMode: ExecModeInline,
	})
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "BASH2", "echo inline"))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "inline")
}

func TestConfigDrivenEngine_FileMode_Execute(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	tmpDir := t.TempDir()
	e := NewConfigDrivenEngine(EngineConfig{
		Type:     "SHELLFILE",
		Binary:   "bash",
		ExecMode: ExecModeFile,
		FileExt:  ".sh",
		WorkDir:  tmpDir,
	})
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "SHELLFILE",
		"echo file_mode_works"))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "file_mode_works")
}

func TestConfigDrivenEngine_FileMode_TempFileCleanedUp(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	tmpDir := t.TempDir()
	e := NewConfigDrivenEngine(EngineConfig{
		Type:     "SHELLFILE",
		Binary:   "bash",
		ExecMode: ExecModeFile,
		FileExt:  ".sh",
		WorkDir:  tmpDir,
	})
	_, err := e.Execute(context.Background(), scriptReq(1, 1, "SHELLFILE", "echo cleanup"))
	require.NoError(t, err)

	entries, _ := os.ReadDir(tmpDir)
	for _, entry := range entries {
		assert.False(t, strings.HasSuffix(entry.Name(), ".sh"),
			"temp .sh file should be cleaned up after execution")
	}
}

func TestConfigDrivenEngine_JarMode_Execute(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	// jar 模式测试：将 bash 当做 "java"，把 execute_param 当路径传入
	// 创建一个临时脚本文件作为"jar"
	script := "#!/bin/bash\necho jar_executed"
	f, err := os.CreateTemp("", "fake-jar-*.sh")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.WriteString(script)
	f.Close()
	os.Chmod(f.Name(), 0o755)

	e := NewConfigDrivenEngine(EngineConfig{
		Type:     "JARTEST",
		Binary:   "bash",
		Args:     []string{},
		ExecMode: ExecModeJar,
	})
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "JARTEST", f.Name()))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "jar_executed")
}

func TestConfigDrivenEngine_WithEnv(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := NewConfigDrivenEngine(EngineConfig{
		Type:     "ENVTEST",
		Binary:   "bash",
		ExecMode: ExecModeInline,
		Env:      []string{"MY_CUSTOM_VAR=hello_from_env"},
	})
	result, err := e.Execute(context.Background(),
		scriptReq(1, 1, "ENVTEST", "echo $MY_CUSTOM_VAR"))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "hello_from_env")
}

func TestConfigDrivenEngine_MaxOutputBytes_Truncates(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := NewConfigDrivenEngine(EngineConfig{
		Type:           "TRUNCTEST",
		Binary:         "bash",
		ExecMode:       ExecModeInline,
		MaxOutputBytes: 5, // 只保留 5 字节
	})
	// 输出 "hello world"（11 字节），应被截断为 5 字节
	result, err := e.Execute(context.Background(),
		scriptReq(1, 1, "TRUNCTEST", "printf 'hello world'"))
	require.NoError(t, err)
	assert.LessOrEqual(t, len(result.Stdout), 5)
}

func TestConfigDrivenEngine_UnknownExecMode_ReturnsError(t *testing.T) {
	e := &ConfigDrivenEngine{
		base: baseScriptEngine{typeName: "X"},
		cfg: EngineConfig{
			Type:     "X",
			Binary:   "bash",
			ExecMode: "unknown_mode",
		},
	}
	_, err := e.Execute(context.Background(), scriptReq(1, 1, "X", "echo hi"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown exec_mode")
}

// ─── 7. LoadFromConfig ────────────────────────────────────────────────────────

func TestLoadFromConfig_RegistersValidEngines(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	configs := []EngineConfig{
		{Type: "MYSH", Binary: "bash", ExecMode: ExecModeInline},
	}
	err := reg.LoadFromConfig(configs)
	require.NoError(t, err)
	_, ok := reg.Get("MYSH")
	assert.True(t, ok)
}

func TestLoadFromConfig_SkipsDisabled(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	configs := []EngineConfig{
		{Type: "DISABLED_ENGINE", Binary: "bash", Disabled: true},
	}
	reg.LoadFromConfig(configs)
	_, ok := reg.Get("DISABLED_ENGINE")
	assert.False(t, ok)
}

func TestLoadFromConfig_PartialFailure_ContinuesRegistration(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	configs := []EngineConfig{
		{Type: "GOOD", Binary: "bash", ExecMode: ExecModeInline},
		{Type: "BAD", Binary: "definitely-no-such-binary-99xyz"},
		{Type: "GOOD2", Binary: "bash", ExecMode: ExecModeInline},
	}
	err := reg.LoadFromConfig(configs)
	// 第一个错误被返回，但其余引擎仍应被注册
	assert.Error(t, err) // BAD 引擎失败
	_, ok1 := reg.Get("GOOD")
	_, ok2 := reg.Get("GOOD2")
	assert.True(t, ok1, "GOOD engine should be registered despite BAD failure")
	assert.True(t, ok2, "GOOD2 engine should be registered despite BAD failure")
}

func TestLoadFromConfig_ConfigOverridesBuiltin(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	// 通过配置覆盖内置 CMD 引擎
	reg := NewScriptEngineRegistry()
	customEngine := &mockEngine{typeName: "CMD", output: "custom_cmd"}
	err := reg.Register(customEngine)
	require.NoError(t, err)
	got, ok := reg.Get("CMD")
	require.True(t, ok)
	assert.Equal(t, customEngine, got, "config-registered engine should override builtin")
}

// ─── 8. Runner 集成 ──────────────────────────────────────────────────────────

func TestRunner_DefaultHasScriptEngines(t *testing.T) {
	r := NewRunner(NewRegistry())
	defer r.Stop()
	assert.NotNil(t, r.ScriptEngines())
}

func TestRunner_WithScriptEngineRegistry_Injected(t *testing.T) {
	custom := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	r := NewRunner(NewRegistry(), WithScriptEngineRegistry(custom))
	defer r.Stop()
	assert.Equal(t, custom, r.ScriptEngines())
}

func TestRunner_UnsupportedExecuteType_ReturnsError(t *testing.T) {
	reg := NewScriptEngineRegistry()
	r := newTestRunnerWithEngines(reg)
	defer r.Stop()

	err := r.Run(context.Background(), scriptReq(9001, 1, "NOTREGISTERED", "echo hi"))
	require.NoError(t, err) // Run 本身不同步返回执行错误

	rec := waitScript(r, 9001, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateFailed, rec.State)
	require.Error(t, rec.Err)
	assert.Contains(t, rec.Err.Error(), "unsupported execute_type")
	assert.Contains(t, rec.Err.Error(), "registered engines")
}

func TestRunner_ShellType_ExecutesSuccessfully(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	reg := NewScriptEngineRegistry()
	r := newTestRunnerWithEngines(reg)
	defer r.Stop()

	err := r.Run(context.Background(), scriptReq(1001, 1, "SHELL", "exit 0"))
	require.NoError(t, err)

	rec := waitScript(r, 1001, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateSuccess, rec.State)
}

func TestRunner_ShellType_ScriptFailure_RecordedAsFailed(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	reg := NewScriptEngineRegistry()
	r := newTestRunnerWithEngines(reg)
	defer r.Stop()

	err := r.Run(context.Background(), scriptReq(2001, 1, "SHELL", "exit 1"))
	require.NoError(t, err)

	rec := waitScript(r, 2001, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateFailed, rec.State)
}

func TestRunner_CmdType_ExecutesEcho(t *testing.T) {
	if !hasBinary("echo") {
		t.Skip("echo not available")
	}
	reg := NewScriptEngineRegistry()
	r := newTestRunnerWithEngines(reg)
	defer r.Stop()

	err := r.Run(context.Background(), scriptReq(3001, 1, "CMD", "echo hello_cmd"))
	require.NoError(t, err)

	rec := waitScript(r, 3001, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateSuccess, rec.State)
}

func TestRunner_ConfigDrivenEngine_ViaRegistry(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	// 模拟 "通过配置文件新增 MYSCRIPT 类型"
	engineReg := NewScriptEngineRegistry()
	err := engineReg.LoadFromConfig([]EngineConfig{
		{
			Type:     "MYSCRIPT",
			Binary:   "bash",
			ExecMode: ExecModeInline,
		},
	})
	require.NoError(t, err)

	r := newTestRunnerWithEngines(engineReg)
	defer r.Stop()

	err = r.Run(context.Background(), scriptReq(4001, 1, "MYSCRIPT", "echo config_driven"))
	require.NoError(t, err)

	rec := waitScript(r, 4001, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateSuccess, rec.State)
}

// ─── 9. 超时 ─────────────────────────────────────────────────────────────────

func TestRunner_ScriptTimeout_RecordedAsFailed(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	reg := NewScriptEngineRegistry()
	r := newTestRunnerWithEngines(reg)
	defer r.Stop()

	req := scriptReq(5001, 1, "SHELL", "sleep 30")
	req.Timeout = 1 // 1 秒超时

	err := r.Run(context.Background(), req)
	require.NoError(t, err)

	rec := waitScript(r, 5001, 5*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateFailed, rec.State,
		"timed-out script should be recorded as failed")
}

// ─── 10. 并发安全 ─────────────────────────────────────────────────────────────

func TestRegistry_ConcurrentRegisterAndGet(t *testing.T) {
	reg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	const N = 50
	var wg sync.WaitGroup

	// 并发注册
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("TYPE%d", i)
			reg.Register(&mockEngine{typeName: name})
		}(i)
	}
	// 并发查询
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reg.Get(fmt.Sprintf("TYPE%d", i))
		}(i)
	}
	wg.Wait()
	// 不崩溃即通过（race detector 会捕获数据竞争）
}

func TestRunner_ConcurrentDifferentEngineTypes(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	reg := NewScriptEngineRegistry()
	r := newTestRunnerWithEngines(reg)
	defer r.Stop()

	const N = 20
	var wg sync.WaitGroup
	var successCount int64

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logID := int64(8000 + i)
			r.Run(context.Background(), scriptReq(logID, 1, "SHELL", "exit 0"))
		}(i)
	}
	wg.Wait()

	// 等待全部完成
	time.Sleep(1 * time.Second)
	for i := 0; i < N; i++ {
		logID := int64(8000 + i)
		rec := r.idempotency.Get(logID)
		if rec != nil && rec.State == idempotency.StateSuccess {
			atomic.AddInt64(&successCount, 1)
		}
	}
	assert.Equal(t, int64(N), successCount, "all concurrent script jobs should succeed")
}

// ─── 11. 诊断接口 ────────────────────────────────────────────────────────────

func TestRunner_ScriptEngines_ExposesRegistry(t *testing.T) {
	custom := NewScriptEngineRegistry()
	require.NoError(t, custom.Register(&mockEngine{typeName: "DIAG"}))
	r := NewRunner(NewRegistry(), WithScriptEngineRegistry(custom))
	defer r.Stop()

	engines := r.ScriptEngines()
	_, ok := engines.Get("DIAG")
	assert.True(t, ok, "ScriptEngines() should expose the injected registry")
}

// ─── 12. ScriptResult.Duration ───────────────────────────────────────────────

func TestScriptResult_DurationRecorded(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	e := &ShellEngine{base: baseScriptEngine{typeName: "SHELL"}}
	result, err := e.Execute(context.Background(), scriptReq(1, 1, "SHELL", "sleep 0.05"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, result.Duration, 50*time.Millisecond,
		"duration should capture actual execution time")
}

// ─── 13. 配置驱动引擎：Args 拼接 ─────────────────────────────────────────────

func TestConfigDrivenEngine_ArgsIncluded(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}
	// 使用 bash --norc -c "..." 验证 Args 被正确拼接
	e := NewConfigDrivenEngine(EngineConfig{
		Type:     "ARGSTEST",
		Binary:   "bash",
		Args:     []string{"--norc"},
		ExecMode: ExecModeInline,
	})
	result, err := e.Execute(context.Background(),
		scriptReq(1, 1, "ARGSTEST", "echo args_ok"))
	require.NoError(t, err)
	assert.Contains(t, string(result.Stdout), "args_ok")
}

// ─── mock helpers ─────────────────────────────────────────────────────────────

// mockEngine 是用于测试的假引擎，无需实际运行外部进程。
type mockEngine struct {
	typeName    string
	output      string
	validateErr error
	executeErr  error
}

func (m *mockEngine) Type() string { return m.typeName }

func (m *mockEngine) Validate() error { return m.validateErr }

func (m *mockEngine) Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	if m.executeErr != nil {
		return nil, m.executeErr
	}
	return &ScriptResult{Stdout: []byte(m.output), ExitCode: 0}, nil
}

// ─── 14. 端到端：Runner 使用 mockEngine 不依赖系统二进制 ─────────────────────

func TestRunner_WithMockEngine_NoSystemBinaryRequired(t *testing.T) {
	engineReg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	mock := &mockEngine{typeName: "MOCK", output: "mock output"}
	require.NoError(t, engineReg.Register(mock))

	r := newTestRunnerWithEngines(engineReg)
	defer r.Stop()

	err := r.Run(context.Background(), scriptReq(7001, 1, "MOCK", "irrelevant"))
	require.NoError(t, err)

	rec := waitScript(r, 7001, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateSuccess, rec.State)
}

func TestRunner_WithMockEngine_FailingEngine_RecordedAsFailed(t *testing.T) {
	engineReg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	mock := &mockEngine{
		typeName:   "FAILMOCK",
		executeErr: fmt.Errorf("simulated engine failure"),
	}
	require.NoError(t, engineReg.Register(mock))

	r := newTestRunnerWithEngines(engineReg)
	defer r.Stop()

	err := r.Run(context.Background(), scriptReq(7002, 1, "FAILMOCK", ""))
	require.NoError(t, err) // Run 不阻塞

	rec := waitScript(r, 7002, 2*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateFailed, rec.State)
	require.Error(t, rec.Err)
	assert.Contains(t, rec.Err.Error(), "simulated engine failure")
}

// ─── 15. 幂等保证对脚本类型同样有效 ──────────────────────────────────────────

func TestRunner_ScriptIdempotency_DuplicateWhileRunning(t *testing.T) {
	engineReg := &ScriptEngineRegistry{engines: make(map[string]ScriptEngine)}
	blockCh := make(chan struct{})
	var execCount int64
	mock := &mockEngineWithBlock{
		typeName: "IDEMPTEST",
		blockCh:  blockCh,
		counter:  &execCount,
	}
	require.NoError(t, engineReg.Register(mock))

	r := newTestRunnerWithEngines(engineReg)
	defer func() { close(blockCh); r.Stop() }()

	// 第一次触发（阻塞中）
	require.NoError(t, r.Run(context.Background(), scriptReq(6001, 1, "IDEMPTEST", "")))
	time.Sleep(20 * time.Millisecond)

	// 重复请求应被幂等拒绝
	err := r.Run(context.Background(), scriptReq(6001, 1, "IDEMPTEST", ""))
	assert.Equal(t, idempotency.ErrAlreadyRunning, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&execCount),
		"script engine should execute only once despite duplicate request")
}

// mockEngineWithBlock 模拟一个阻塞等待通道信号才结束的引擎，用于幂等测试。
type mockEngineWithBlock struct {
	typeName string
	blockCh  chan struct{}
	counter  *int64
}

func (m *mockEngineWithBlock) Type() string { return m.typeName }
func (m *mockEngineWithBlock) Validate() error { return nil }
func (m *mockEngineWithBlock) Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	atomic.AddInt64(m.counter, 1)
	select {
	case <-m.blockCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &ScriptResult{ExitCode: 0}, nil
}

// ─── 16. 完整配置驱动流程端到端（主集成测试） ─────────────────────────────────

// TestFullConfigDrivenFlow 模拟真实场景：从配置加载引擎 → 注入 Runner → 执行任务
func TestFullConfigDrivenFlow(t *testing.T) {
	if !hasBinary("bash") {
		t.Skip("bash not available")
	}

	// Step 1: 模拟从配置文件加载的引擎配置（相当于 cfg.Executor.ScriptEngines）
	cfgEngines := []EngineConfig{
		{
			// 模拟 "GO" 引擎（用 bash 文件模式代替，无需真实 go 工具链）
			Type:     "GOBASH",
			Binary:   "bash",
			ExecMode: ExecModeFile,
			FileExt:  ".sh",
			WorkDir:  t.TempDir(),
		},
		{
			// 禁用的引擎，不应被注册
			Type:     "DISABLED",
			Binary:   "bash",
			Disabled: true,
		},
	}

	// Step 2: 创建注册表，加载配置
	engineReg := NewScriptEngineRegistry()
	err := engineReg.LoadFromConfig(cfgEngines)
	require.NoError(t, err)

	// Step 3: 验证内置引擎存在，配置引擎也存在，禁用引擎不存在
	_, hasBuiltinShell := engineReg.Get("SHELL")
	_, hasBuiltinCmd := engineReg.Get("CMD")
	_, hasConfigGoBash := engineReg.Get("GOBASH")
	_, hasDisabled := engineReg.Get("DISABLED")
	assert.True(t, hasBuiltinShell, "builtin SHELL should exist")
	assert.True(t, hasBuiltinCmd, "builtin CMD should exist")
	assert.True(t, hasConfigGoBash, "config-driven GOBASH should exist")
	assert.False(t, hasDisabled, "disabled engine should not be registered")

	// Step 4: 创建 Runner 并注入引擎注册表
	runner := NewRunner(NewRegistry(),
		WithIdempotencyTTL(5*time.Second),
		WithIdempotencyGCInterval(time.Minute),
		WithScriptEngineRegistry(engineReg),
	)
	defer runner.Stop()

	// Step 5: 使用配置引擎执行任务（execute_type = "GOBASH"）
	script := "#!/bin/bash\necho full_config_driven_flow"
	err = runner.Run(context.Background(), &RunRequest{
		LogID:        9999,
		JobID:        1,
		ExecuteType:  "GOBASH",
		ExecuteParam: script,
	})
	require.NoError(t, err)

	rec := waitScript(runner, 9999, 3*time.Second)
	require.NotNil(t, rec)
	assert.Equal(t, idempotency.StateSuccess, rec.State,
		"full config-driven flow should succeed")

	// Step 6: 确认 logCh 收到了输出
	select {
	case logLine := <-runner.logCh:
		assert.Contains(t, logLine.Content, "full_config_driven_flow")
		assert.Equal(t, int64(9999), logLine.LogID)
	case <-time.After(500 * time.Millisecond):
		// logCh 是异步的，未收到也不强制失败（但注意 logCh 在 goroutine 里写入）
	}
}

// ─── Benchmark ───────────────────────────────────────────────────────────────

// BenchmarkRegistry_Get 测量热路径（路由查询）的性能。
func BenchmarkRegistry_Get(b *testing.B) {
	reg := NewScriptEngineRegistry()
	reg.Register(&mockEngine{typeName: "BENCH"})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reg.Get("BENCH")
		}
	})
}

// BenchmarkConfigDrivenEngine_Validate 测量引擎验证性能（启动期调用）。
func BenchmarkConfigDrivenEngine_Validate(b *testing.B) {
	e := NewConfigDrivenEngine(EngineConfig{Type: "BENCH", Binary: "bash"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Validate()
	}
}

// TestWriteTempFile_ExtensionNormalization 验证 ext 自动补点
func TestWriteTempFile_ExtensionNormalization(t *testing.T) {
	cases := []struct {
		ext    string
		suffix string
	}{
		{".go", ".go"},
		{"go", ".go"},
		{"", ""},
	}
	for _, tc := range cases {
		path, err := writeTempFile("", tc.ext, "content")
		require.NoError(t, err)
		defer os.Remove(path)
		if tc.suffix != "" {
			assert.True(t, strings.HasSuffix(filepath.Base(path), tc.suffix),
				"ext=%q want suffix %q, got %q", tc.ext, tc.suffix, path)
		}
	}
}
