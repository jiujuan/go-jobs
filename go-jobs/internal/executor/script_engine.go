// Package executor - script_engine.go
//
// # 脚本执行引擎（策略模式 + 注册表模式 + 配置驱动）
//
// # 设计模式
//
//   - 策略模式（Strategy Pattern）：ScriptEngine 接口定义统一的执行契约，
//     每种语言/脚本类型对应一个独立策略实现（ShellEngine / PythonEngine 等）。
//     Runner 持有 ScriptEngineRegistry，通过类型名称委托给对应策略，
//     运行时可无缝增删策略实现，不修改调用方代码。
//
//   - 注册表模式（Registry Pattern）：ScriptEngineRegistry 集中管理所有引擎。
//     内置引擎在进程启动时注册；配置驱动引擎在加载配置后动态注册。
//     注册与使用完全解耦，新增引擎只需一次 Register() 调用。
//
//   - 工厂方法（Factory Method）：NewScriptEngineRegistry 负责实例化注册表
//     并注册内置引擎；NewConfigDrivenEngine 根据 EngineConfig 创建配置驱动引擎。
//
// # 扩展方式（零代码改动）
//
//  1. 在配置文件中添加 script_engines 块：
//
//     executor:
//       script_engines:
//         - type: go
//           binary: go
//           args: ["run"]
//           file_ext: .go
//           work_dir: /tmp/go-jobs
//           env:
//             GOPATH: /usr/local/go
//         - type: java
//           binary: java
//           args: ["-jar"]
//           file_ext: .jar
//
//  2. 在 main.go 中加载配置后调用一次：
//
//     engineRegistry.LoadFromConfig(cfg.Executor.ScriptEngines)
//
//  3. 在调度平台界面将任务的 execute_type 设为 "GO"（或任意已配置的类型），
//     无需任何代码变更。
//
// # 执行模式
//
// ScriptEngine 支持两种执行模式，由 EngineConfig.ExecMode 控制：
//
//   - inline：将 execute_param 作为内联代码传给解释器（bash -c "..."）
//     适合：SHELL、PYTHON、Groovy 等脚本语言
//
//   - file：将 execute_param 写入临时文件后执行，执行完清理
//     适合：Go、Java（源码）、需要文件路径的解释器
//
//   - jar：execute_param 是 .jar 文件路径，直接传给 java -jar
//     适合：Java 打包后的制品执行
package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/pkg/logger"
)

// ─── 执行模式常量 ─────────────────────────────────────────────────────────────

const (
	// ExecModeInline：将 execute_param 作为代码内容传给解释器（-c 参数）
	ExecModeInline = "inline"
	// ExecModeFile：将 execute_param 写入临时文件，解释器读文件执行
	ExecModeFile = "file"
	// ExecModeJar：execute_param 是可执行文件路径，直接传给运行时
	ExecModeJar = "jar"
)

// ─── ScriptResult ─────────────────────────────────────────────────────────────

// ScriptResult 保存单次脚本执行的完整结果。
type ScriptResult struct {
	// Stdout 标准输出内容
	Stdout []byte
	// Stderr 标准错误内容
	Stderr []byte
	// ExitCode 进程退出码（0 = 成功）
	ExitCode int
	// Duration 实际执行耗时
	Duration time.Duration
}

// Combined 返回合并的输出内容（stdout + stderr），供日志记录使用。
func (r *ScriptResult) Combined() string {
	buf := make([]byte, 0, len(r.Stdout)+len(r.Stderr)+1)
	buf = append(buf, r.Stdout...)
	if len(r.Stdout) > 0 && len(r.Stderr) > 0 {
		buf = append(buf, '\n')
	}
	buf = append(buf, r.Stderr...)
	return string(buf)
}

// ─── ScriptEngine 接口（策略）─────────────────────────────────────────────────

// ScriptEngine 定义脚本执行引擎的统一契约（策略接口）。
//
// 每种语言/脚本类型实现此接口：
//   - ShellEngine   → SHELL
//   - PythonEngine  → PYTHON
//   - CmdEngine     → CMD
//   - ConfigDrivenEngine → GO / JAVA / CSHARP / 任意配置的类型
type ScriptEngine interface {
	// Type 返回此引擎处理的执行类型名称（大写），如 "SHELL"、"GO"。
	Type() string

	// Execute 执行一次任务。
	//   ctx:   带有 deadline 的 context，超时时子进程会被 kill
	//   req:   来自调度器的完整触发请求
	// 返回 ScriptResult 和 error，error 非 nil 时调度器会记录失败并触发重试/告警。
	Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error)

	// Validate 在注册时校验引擎配置是否合法（如 binary 是否存在、权限是否满足）。
	// 返回 error 时注册被拒绝，便于启动期快速失败。
	Validate() error
}

// ─── EngineConfig ─────────────────────────────────────────────────────────────

// EngineConfig 是配置文件中单个引擎的配置结构。
// 对应 executor.script_engines[] 数组的每一项。
type EngineConfig struct {
	// Type 引擎类型名称（大写），作为 execute_type 的值使用，如 "GO" "JAVA" "CSHARP"
	Type string `mapstructure:"type" yaml:"type"`

	// Binary 可执行程序路径，可以是 PATH 上的命令名，也可以是绝对路径
	// 示例：go / java / dotnet / /usr/bin/ruby
	Binary string `mapstructure:"binary" yaml:"binary"`

	// Args execute_param 之前的固定参数列表
	// 示例：go:["run"] → 执行 go run {param}
	//       java:["--enable-preview", "-jar"] → 执行 java --enable-preview -jar {param}
	Args []string `mapstructure:"args" yaml:"args"`

	// FileExt 临时文件扩展名（ExecModeFile 下使用），如 ".go" ".py" ".cs"
	// 不含点时自动补全
	FileExt string `mapstructure:"file_ext" yaml:"file_ext"`

	// WorkDir 脚本工作目录。空字符串使用系统默认临时目录
	WorkDir string `mapstructure:"work_dir" yaml:"work_dir"`

	// Env 追加到子进程的额外环境变量，格式 ["KEY=VALUE", ...]
	// 不填写时继承父进程环境
	Env []string `mapstructure:"env" yaml:"env"`

	// ExecMode 执行模式：inline（默认）/ file / jar
	ExecMode string `mapstructure:"exec_mode" yaml:"exec_mode"`

	// MaxOutputBytes 单次执行最大输出字节数（0 = 不限制，建议生产设置 4MB）
	MaxOutputBytes int64 `mapstructure:"max_output_bytes" yaml:"max_output_bytes"`

	// Disabled 设为 true 时跳过该引擎注册（灰度/临时禁用某类任务）
	Disabled bool `mapstructure:"disabled" yaml:"disabled"`
}

// ─── ScriptEngineRegistry（注册表）────────────────────────────────────────────

// ScriptEngineRegistry 集中管理所有脚本引擎，并发安全。
//
// 生命周期：
//
//	registry := NewScriptEngineRegistry()          // 注册内置引擎
//	registry.LoadFromConfig(cfg.ScriptEngines)     // 注册配置驱动引擎
//	runner := NewRunner(beanRegistry, WithScriptEngineRegistry(registry))
type ScriptEngineRegistry struct {
	mu      sync.RWMutex
	engines map[string]ScriptEngine // key = Type（已 ToUpper）
}

// NewScriptEngineRegistry 创建注册表并自动注册全部内置引擎。
// 内置引擎（SHELL / PYTHON / CMD）始终可用，无需配置文件。
func NewScriptEngineRegistry() *ScriptEngineRegistry {
	r := &ScriptEngineRegistry{
		engines: make(map[string]ScriptEngine),
	}
	// 注册内置引擎
	for _, e := range builtinEngines() {
		if err := r.Register(e); err != nil {
			// 内置引擎配置固定，不应出错；打印警告继续
			logger.Warn("script_engine: builtin engine validate failed",
				zap.String("type", e.Type()), zap.Error(err))
		}
	}
	return r
}

// Register 注册一个引擎。若类型已注册则覆盖（支持配置文件覆盖内置引擎）。
// 注册前调用 engine.Validate()，失败时返回错误，引擎不被注册。
func (r *ScriptEngineRegistry) Register(engine ScriptEngine) error {
	if engine == nil {
		return fmt.Errorf("script_engine: register nil engine")
	}
	typeName := strings.ToUpper(engine.Type())
	if typeName == "" {
		return fmt.Errorf("script_engine: engine type name is empty")
	}
	if err := engine.Validate(); err != nil {
		return fmt.Errorf("script_engine: validate %q: %w", typeName, err)
	}
	r.mu.Lock()
	r.engines[typeName] = engine
	r.mu.Unlock()
	logger.Info("script_engine: registered", zap.String("type", typeName))
	return nil
}

// Get 获取指定类型的引擎，不存在返回 nil, false。
func (r *ScriptEngineRegistry) Get(typeName string) (ScriptEngine, bool) {
	r.mu.RLock()
	e, ok := r.engines[strings.ToUpper(typeName)]
	r.mu.RUnlock()
	return e, ok
}

// Types 返回所有已注册引擎的类型名列表（排序）。
func (r *ScriptEngineRegistry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.engines))
	for k := range r.engines {
		names = append(names, k)
	}
	return names
}

// LoadFromConfig 从配置列表批量注册引擎。
// 跳过 Disabled=true 的配置项；部分失败时记录警告并继续注册其余引擎。
// 返回遇到的第一个错误（不影响成功注册的引擎）。
func (r *ScriptEngineRegistry) LoadFromConfig(configs []EngineConfig) error {
	var firstErr error
	for _, cfg := range configs {
		if cfg.Disabled {
			logger.Info("script_engine: skipping disabled engine",
				zap.String("type", cfg.Type))
			continue
		}
		engine := NewConfigDrivenEngine(cfg)
		if err := r.Register(engine); err != nil {
			logger.Warn("script_engine: failed to register config-driven engine",
				zap.String("type", cfg.Type),
				zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// ─── baseScriptEngine：通用执行逻辑 ──────────────────────────────────────────

// baseScriptEngine 提供公共的子进程执行逻辑，避免各实现重复代码。
// 内置引擎和配置驱动引擎均嵌入此结构体。
type baseScriptEngine struct {
	typeName       string
	maxOutputBytes int64 // 0 = 不限制
}

// runProcess 通用子进程执行：启动命令、收集输出、处理超时。
func (b *baseScriptEngine) runProcess(
	ctx context.Context,
	binary string,
	args []string,
	env []string, // 追加的额外环境变量，nil = 继承父进程
) (*ScriptResult, error) {
	start := time.Now()

	cmd := exec.CommandContext(ctx, binary, args...)

	// 继承父进程环境并追加自定义变量
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	if b.maxOutputBytes > 0 {
		cmd.Stdout = &limitedWriter{buf: &stdoutBuf, limit: b.maxOutputBytes}
		cmd.Stderr = &limitedWriter{buf: &stderrBuf, limit: b.maxOutputBytes}
	} else {
		cmd.Stdout = &stdoutBuf
		cmd.Stderr = &stderrBuf
	}

	runErr := cmd.Run()
	dur := time.Since(start)

	exitCode := 0
	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	result := &ScriptResult{
		Stdout:   stdoutBuf.Bytes(),
		Stderr:   stderrBuf.Bytes(),
		ExitCode: exitCode,
		Duration: dur,
	}

	if runErr != nil {
		return result, fmt.Errorf(
			"script engine %q: exit_code=%d, err=%w, output=%s",
			b.typeName, exitCode, runErr, result.Combined(),
		)
	}

	if len(result.Combined()) > 0 {
		logger.Info("script_engine: execution output",
			zap.String("type", b.typeName),
			zap.String("output", result.Combined()))
	}

	return result, nil
}

// writeTempFile 将内容写入临时文件，返回文件路径。调用方负责 os.Remove。
func writeTempFile(dir, ext, content string) (string, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return "", fmt.Errorf("script_engine: mkdir work_dir %q: %w", dir, err)
		}
	}
	if ext != "" && !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	f, err := os.CreateTemp(dir, "go-jobs-script-*"+ext)
	if err != nil {
		return "", fmt.Errorf("script_engine: create temp file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("script_engine: write temp file: %w", err)
	}
	return f.Name(), nil
}

// ─── limitedWriter：输出大小限制 ─────────────────────────────────────────────

// limitedWriter 截断超出 limit 字节的输出，防止内存耗尽。
type limitedWriter struct {
	buf     *bytes.Buffer
	limit   int64
	written int64
}

func (w *limitedWriter) Write(p []byte) (n int, err error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		return len(p), nil // 静默丢弃超出部分
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err = w.buf.Write(p)
	w.written += int64(n)
	return len(p), err // 返回原始 len，不让 cmd.Run 出错
}

// ─── 内置引擎 ─────────────────────────────────────────────────────────────────

// builtinEngines 返回所有内置引擎实例。
// 内置引擎与旧版行为完全兼容，不依赖任何配置文件。
func builtinEngines() []ScriptEngine {
	return []ScriptEngine{
		&ShellEngine{base: baseScriptEngine{typeName: "SHELL"}},
		&PythonEngine{base: baseScriptEngine{typeName: "PYTHON"}},
		&CmdEngine{base: baseScriptEngine{typeName: "CMD"}},
	}
}

// ── ShellEngine ───────────────────────────────────────────────────────────────

// ShellEngine 通过 bash -c 执行 Shell 脚本（对应 SHELL 类型）。
type ShellEngine struct {
	base baseScriptEngine
}

func (e *ShellEngine) Type() string { return "SHELL" }

func (e *ShellEngine) Validate() error {
	if _, err := exec.LookPath("bash"); err != nil {
		return fmt.Errorf("bash not found in PATH: %w", err)
	}
	return nil
}

func (e *ShellEngine) Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	return e.base.runProcess(ctx, "bash", []string{"-c", req.ExecuteParam}, nil)
}

// ── PythonEngine ──────────────────────────────────────────────────────────────

// PythonEngine 通过 python3 -c 执行 Python 代码（对应 PYTHON 类型）。
type PythonEngine struct {
	base baseScriptEngine
}

func (e *PythonEngine) Type() string { return "PYTHON" }

func (e *PythonEngine) Validate() error {
	if _, err := exec.LookPath("python3"); err != nil {
		return fmt.Errorf("python3 not found in PATH: %w", err)
	}
	return nil
}

func (e *PythonEngine) Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	return e.base.runProcess(ctx, "python3", []string{"-c", req.ExecuteParam}, nil)
}

// ── CmdEngine ─────────────────────────────────────────────────────────────────

// CmdEngine 将 execute_param 解析为命令行并直接执行（对应 CMD 类型）。
// execute_param 示例："ls -la /tmp" 或 "/usr/bin/myapp --flag value"
type CmdEngine struct {
	base baseScriptEngine
}

func (e *CmdEngine) Type() string { return "CMD" }

func (e *CmdEngine) Validate() error { return nil }

func (e *CmdEngine) Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	parts := strings.Fields(req.ExecuteParam)
	if len(parts) == 0 {
		return nil, fmt.Errorf("CMD engine: execute_param is empty")
	}
	return e.base.runProcess(ctx, parts[0], parts[1:], nil)
}

// ─── ConfigDrivenEngine（配置驱动引擎，核心扩展点）────────────────────────────

// ConfigDrivenEngine 是通过配置文件定义的通用引擎。
// 一个 ConfigDrivenEngine 实例处理一种配置的执行类型（GO / JAVA / CSHARP / RUBY 等）。
// 无需编写代码，仅靠 YAML 配置即可支持新语言。
type ConfigDrivenEngine struct {
	base baseScriptEngine
	cfg  EngineConfig
}

// NewConfigDrivenEngine 从配置创建引擎实例。
func NewConfigDrivenEngine(cfg EngineConfig) *ConfigDrivenEngine {
	cfg.Type = strings.ToUpper(cfg.Type)
	if cfg.ExecMode == "" {
		cfg.ExecMode = ExecModeInline
	}
	return &ConfigDrivenEngine{
		base: baseScriptEngine{
			typeName:       cfg.Type,
			maxOutputBytes: cfg.MaxOutputBytes,
		},
		cfg: cfg,
	}
}

func (e *ConfigDrivenEngine) Type() string { return e.cfg.Type }

// Validate 校验引擎配置：类型名非空、binary 可找到。
func (e *ConfigDrivenEngine) Validate() error {
	if e.cfg.Type == "" {
		return fmt.Errorf("engine type is empty")
	}
	if e.cfg.Binary == "" {
		return fmt.Errorf("engine %q: binary is empty", e.cfg.Type)
	}
	// 尝试在 PATH 中查找；如果是绝对路径直接检查存在性
	if filepath.IsAbs(e.cfg.Binary) {
		if _, err := os.Stat(e.cfg.Binary); err != nil {
			return fmt.Errorf("engine %q: binary %q not found: %w", e.cfg.Type, e.cfg.Binary, err)
		}
	} else {
		if _, err := exec.LookPath(e.cfg.Binary); err != nil {
			return fmt.Errorf("engine %q: binary %q not in PATH: %w", e.cfg.Type, e.cfg.Binary, err)
		}
	}
	return nil
}

// Execute 根据 ExecMode 选择执行方式：
//
//   - inline：args + ["-c", param]，适合 bash/python/groovy 等解释器
//   - file：将 param 写入临时文件，args + [filePath]，适合 go run / ruby / 编译型语言源码
//   - jar：args + [param]，param 是 .jar 或可执行制品的路径，适合 java -jar
func (e *ConfigDrivenEngine) Execute(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	switch e.cfg.ExecMode {
	case ExecModeInline:
		return e.executeInline(ctx, req)
	case ExecModeFile:
		return e.executeFile(ctx, req)
	case ExecModeJar:
		return e.executeJar(ctx, req)
	default:
		return nil, fmt.Errorf("engine %q: unknown exec_mode %q", e.cfg.Type, e.cfg.ExecMode)
	}
}

// executeInline：binary args... -c param
func (e *ConfigDrivenEngine) executeInline(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	args := make([]string, 0, len(e.cfg.Args)+2)
	args = append(args, e.cfg.Args...)
	args = append(args, "-c", req.ExecuteParam)
	return e.base.runProcess(ctx, e.cfg.Binary, args, e.cfg.Env)
}

// executeFile：写临时文件后 binary args... filePath，执行后自动清理
func (e *ConfigDrivenEngine) executeFile(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	tmpPath, err := writeTempFile(e.cfg.WorkDir, e.cfg.FileExt, req.ExecuteParam)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpPath)

	args := make([]string, 0, len(e.cfg.Args)+1)
	args = append(args, e.cfg.Args...)
	args = append(args, tmpPath)
	return e.base.runProcess(ctx, e.cfg.Binary, args, e.cfg.Env)
}

// executeJar：binary args... param（param 是文件路径/制品路径）
func (e *ConfigDrivenEngine) executeJar(ctx context.Context, req *RunRequest) (*ScriptResult, error) {
	args := make([]string, 0, len(e.cfg.Args)+1)
	args = append(args, e.cfg.Args...)
	args = append(args, req.ExecuteParam)
	return e.base.runProcess(ctx, e.cfg.Binary, args, e.cfg.Env)
}
