# Script 执行引擎重构设计文档

> **模块**：`internal/executor` — 脚本执行引擎  
> **版本**：v3.5  
> **涉及文件**：`script_engine.go` · `job_runner.go` · `pkg/conf/conf.go` · `config/executor.dev.yaml`

---

## 1. 问题分析：旧设计的缺陷

### 旧版 `job_runner.go` 的问题

```go
// 旧版 Run()：每新增一种语言，必须改这个 switch ──── 违反开放/封闭原则
switch strings.ToUpper(req.ExecuteType) {
case "BEAN":   err = r.runBean(execCtx, req)
case "SHELL":  err = r.runShell(execCtx, req)
case "PYTHON": err = r.runPython(execCtx, req)
case "CMD":    err = r.runCmd(execCtx, req)
default:       err = fmt.Errorf("unsupported execute type: %s", req.ExecuteType)
}
```

**缺陷一览：**

| 缺陷 | 说明 |
|------|------|
| 违反开放/封闭原则 | 新增执行类型必须修改 `Run()` 函数，存在引入 bug 的风险 |
| 不可配置 | 解释器路径（如 `python3`）硬编码，无法通过配置指定环境变量、参数 |
| 无扩展机制 | 企业内部语言（Groovy、Perl）或新运行时（dotnet）无法接入 |
| 测试困难 | 各语言执行逻辑耦合在 Runner，单独测试一种引擎需要启动整个 Runner |
| 输出管理缺失 | 无大小限制，脚本输出内存暴增无法控制 |

---

## 2. 设计模式选型

重构后的脚本执行引擎同时运用了三种设计模式：

### 2.1 策略模式（Strategy Pattern）——核心

**意图**：定义一系列算法（执行策略），将每种算法封装起来，使它们可以互换，且算法的变化不影响使用算法的客户。

```
        ┌──────────────────┐
        │   <<interface>>  │
        │   ScriptEngine   │◀─────────────────────────┐
        │                  │                          │
        │ + Type() string  │                          │
        │ + Execute(...)   │        context           │
        │ + Validate()     │        ┌──────────────┐  │
        └──────────────────┘        │    Runner     │──┘
               ▲    ▲   ▲           │               │
       ┌───────┘    │   └────────┐  │ runScript()   │
       │            │            │  │  engine :=    │
 ┌─────────┐ ┌────────────┐ ┌─────────────┐  Get(type) │
 │ Shell-  │ │ Python-    │ │ Config-     │  └──────────────┘
 │ Engine  │ │ Engine     │ │ DrivenEngine│
 │(SHELL)  │ │(PYTHON)    │ │(GO/JAVA/C#) │
 └─────────┘ └────────────┘ └─────────────┘
    内置策略       内置策略       配置驱动策略
```

**关键收益**：`Runner.runScript()` 只调用 `engine.Execute()`，完全不知道具体是哪种引擎。新增 Go/Java/C# 引擎只需增加实现或配置，`Runner` 代码零改动。

---

### 2.2 注册表模式（Registry Pattern）——管理中心

**意图**：维护一个全局（或作用域内）的对象注册中心，按键名查找和获取实例。

```go
// ScriptEngineRegistry 是引擎的注册中心
type ScriptEngineRegistry struct {
    mu      sync.RWMutex
    engines map[string]ScriptEngine  // key = 类型名（大写）
}
```

**设计要点：**
- **注册时校验**（`Validate()`）：启动期快速失败，而非运行时才发现 binary 不存在
- **大写规范化**：`Get("go")` 与 `Get("GO")` 等价，避免配置大小写问题
- **覆盖语义**：配置驱动引擎可覆盖内置引擎（如自定义 Python 环境替换系统默认）
- **并发安全**：`sync.RWMutex`，读多写少场景开销极低

---

### 2.3 工厂方法（Factory Method）——对象创建

**意图**：定义创建对象的接口，由子类/工厂函数决定实例化哪个类。

```go
// 工厂函数：根据 EngineConfig 创建 ConfigDrivenEngine
func NewConfigDrivenEngine(cfg EngineConfig) *ConfigDrivenEngine

// 工厂函数：创建注册表并自动注册所有内置引擎
func NewScriptEngineRegistry() *ScriptEngineRegistry
```

`LoadFromConfig()` 是工厂方法的批量版本，将配置数组转换为已注册的引擎注册表。

---

## 3. 整体架构

```
配置文件 executor.yaml
    │
    │ script_engines: [{type: go, binary: go, ...}, ...]
    ▼
pkg/conf.ScriptEngineConfig  ──── mapstructure ────▶  []EngineConfig
    │
    │ engineReg.LoadFromConfig(cfgEngines)
    ▼
ScriptEngineRegistry  (并发安全注册表)
    ├── "SHELL"  → ShellEngine   (内置)
    ├── "PYTHON" → PythonEngine  (内置)
    ├── "CMD"    → CmdEngine     (内置)
    ├── "GO"     → ConfigDrivenEngine{binary:"go", mode:file, ext:".go"}
    ├── "JAVA"   → ConfigDrivenEngine{binary:"java", mode:jar}
    └── "CSHARP" → ConfigDrivenEngine{binary:"dotnet", mode:file, ext:".csx"}
              │
              │ WithScriptEngineRegistry(engineReg)
              ▼
           Runner
              │
              │  Run(req) → switch req.ExecuteType
              │               "BEAN" → registry.Get(handler) → h(ctx, param)
              │               其他  → scriptEngines.Get(type) → engine.Execute(ctx, req)
              │
    ┌─────────┴────────────────────────┐
    │       ConfigDrivenEngine         │
    │                                  │
    │  ExecMode = "inline"             │
    │   binary args... -c "param"      │
    │                                  │
    │  ExecMode = "file"               │
    │   writeTempFile(param) →         │
    │   binary args... /tmp/xxx.ext →  │
    │   defer os.Remove(tmpPath)       │
    │                                  │
    │  ExecMode = "jar"                │
    │   binary args... param           │
    └──────────────────────────────────┘
```

---

## 4. 三种执行模式详解

| 模式 | 配置值 | 命令拼接方式 | 适用场景 |
|------|--------|-------------|---------|
| **inline** | `exec_mode: inline` | `binary args... -c "execute_param"` | bash / python3 / node -e / groovy -e |
| **file** | `exec_mode: file` | 写临时文件 → `binary args... /tmp/xxx.ext` | go run / dotnet script / ruby 文件 |
| **jar** | `exec_mode: jar` | `binary args... execute_param`（param 是路径） | java -jar / 可执行制品路径 |

---

## 5. 扩展新语言：全程零代码改动

### 示例：新增 Groovy 执行类型

只需在 `executor.yaml` 中添加：

```yaml
executor:
  script_engines:
    - type: groovy          # execute_type 的值
      binary: groovy        # PATH 中的命令名，或绝对路径
      exec_mode: file       # 写临时文件再执行
      file_ext: .groovy     # 临时文件扩展名
      work_dir: /tmp/gj     # 工作目录（空则用系统临时目录）
      max_output_bytes: 4194304
```

然后在调度平台创建任务时将 `execute_type` 设为 `GROOVY`，`execute_param` 填写 Groovy 代码即可。**不需要修改任何 Go 代码**。

### main.go 集成（一次性修改，之后永不改动）

```go
func main() {
    cfg, _ := conf.Load(conf.WithConfigFile("config/executor.yaml"))

    // ① 创建引擎注册表（内置 SHELL/PYTHON/CMD 自动注册）
    engineReg := executor.NewScriptEngineRegistry()

    // ② 从配置加载扩展引擎（GO/JAVA/CSHARP/... 按配置注册）
    engineReg.LoadFromConfig(toEngineConfigs(cfg.Executor.ScriptEngines))

    // ③ 注入 Runner
    runner := executor.NewRunner(beanRegistry,
        executor.WithScriptEngineRegistry(engineReg),
    )
    // 之后新增任何语言，main.go 不需要改动
}
```

---

## 6. 文件变更说明

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `internal/executor/script_engine.go` | **新增** | 策略接口 + 内置引擎 + 配置驱动引擎 + 注册表 |
| `internal/executor/job_runner.go` | **修改** | `runScript()` 委托给注册表；新增 `WithScriptEngineRegistry` 选项 |
| `internal/executor/script_engine_test.go` | **新增** | 76 个测试用例 + 2 个 Benchmark |
| `pkg/conf/conf.go` | **修改** | `ExecutorConfig` 新增 `ScriptEngines []ScriptEngineConfig` 字段 |
| `config/executor.dev.yaml` | **修改** | 新增完整的 `script_engines` 配置块示例 |

---

## 7. 向后兼容性保证

- `BEAN`、`SHELL`、`PYTHON`、`CMD` 四种类型的行为与旧版**完全一致**
- `NewRunner()` 若不传 `WithScriptEngineRegistry`，自动创建含内置引擎的注册表
- 旧版的 `runShell()` / `runPython()` / `runCmd()` 逻辑已内置到对应引擎，无外部接口变化
- 现有的 `runner_test.go` 中所有 BEAN 测试**无需修改**，可直接通过

---

## 8. 测试用例分类

| 分类 | 用例数 | 说明 |
|------|-------|------|
| ScriptResult | 4 | Combined() 各边界情况 |
| Registry CRUD | 8 | Register/Get/Types/LoadFromConfig |
| limitedWriter | 2 | 输出截断边界 |
| writeTempFile | 4 | 临时文件创建/扩展名/目录/清理 |
| 内置引擎 | 12 | Shell/Python/Cmd 的 Validate + Execute |
| ConfigDrivenEngine | 13 | 三种 ExecMode + Env + MaxOutput + Args |
| Runner 集成 | 10 | 注入/路由/超时/失败/并发 |
| 幂等保证 | 2 | 脚本类型同样受幂等保护 |
| 端到端集成 | 2 | 全流程 + logCh 验证 |
| Benchmark | 2 | Get 热路径 + Validate 性能 |

运行方式：

```bash
# 全部测试（含集成，需要 bash 可用）
go test ./internal/executor/... -v -run TestScript

# 仅无需外部二进制的单元测试
go test ./internal/executor/... -v -run "TestScriptResult|TestRegistry|TestLimited|TestWriteTemp|TestConfigDriven.*Type|TestConfigDriven.*Validate|TestConfigDriven.*Default|TestRunner_With|TestRunner_Unsupported|TestFullConfig|TestRunner_WithMock"

# 并发安全（race detector）
go test ./internal/executor/... -race -count=3 -run TestRegistry_Concurrent

# Benchmark
go test ./internal/executor/... -bench=BenchmarkRegistry -benchmem
```
