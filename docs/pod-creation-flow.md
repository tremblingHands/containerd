# Pod Sandbox 创建流程

本文基于 containerd trace 日志和代码实现，说明 `crictl runp` 创建 Pod Sandbox 的完整流程。

---

## 1. 总览

```
crictl runp pod.yaml
  └── CRI gRPC: RunPodSandbox
        └── containerd daemon
              ├── ① 生成 sandbox ID，创建 metadata 记录
              ├── ② 设置 netns（网络命名空间）
              ├── ③ CNI 配置网络（调用 CNI 插件分配 IP）
              ├── ④ 创建 containerd container 对象
              ├── ⑤ 创建 Task（启动 shim 进程 + 创建容器）
              ├── ⑥ 启动 Task
              ├── ⑦ 更新 metadata
              ├── ⑧ 查询 status
              └── ⑨ 启动后台 WaitSandbox（独立 trace）
```

---

## 2. Trace 树与耗时

以下是一次实际沙箱创建的 trace（总耗时 ~166ms）：

```
cri.sandbox.run                                             166ms (100.0%) [self:  8.79ms]
├── metadata.sandbox.Create                                   1.46ms ( 0.9%)
├── metadata.sandbox.Update (#1)                              1.44ms ( 0.9%)
├── cni.setup_pod_network                                    90.3ms (54.4%)
│   └── cni.plugin_setup                                     90.2ms
│       └── cni.attach_networks_serially                     90.1ms
│           └── cni.network.attach                           90.0ms
│               ├── cni.plugin.bridge                        50.0ms
│               ├── cni.plugin.portmap                       15.0ms
│               └── cni.plugin.firewall                      25.0ms
├── metadata.sandbox.Update (#2)                              1.39ms ( 0.8%)
├── client.NewContainer                                      10.9ms ( 6.6%)
│   └── metadata.containers.Create                           0.06ms
├── container.NewTask                                        44.9ms (27.0%)
│   ├── client.task_service.create (gRPC client)             44.6ms
│   └── tasks.create (gRPC server)                           44.5ms
│       └── runtime.task_manager.create                      44.1ms
│           ├── runtime.bundle.create                         0.1ms
│           ├── runtime.mount.activate                        0.2ms
│           ├── shim.manager.start                            6.68ms
│           │   ├── shim.binary.exec                          5.87ms
│           │   └── shim.binary.connect                       0.15ms
│           └── shim.task.create                             36.4ms  (RPC to shim)
├── client.task.Start                                         5.00ms
│   ├── client.task_service.start (gRPC client)               4.8ms
│   └── tasks.start (gRPC server)                             4.6ms
├── metadata.sandbox.Update (#3)                              1.44ms
├── cri.sandbox.status (#1)                                   0.26ms
└── cri.sandbox.status (#2)                                   0.10ms

═══ 独立 trace: task.Wait (detached context) ═══
task.Wait                                                    72.7ms  (WaitSandbox 后台等待 shim 退出)
```

---

## 3. 详细流程

### 3.1 cri.sandbox.run — 入口

**文件**: `internal/cri/server/sandbox_run.go:54`  
**函数**: `(c *criService) RunPodSandbox()`

```go
func (c *criService) RunPodSandbox(ctx context.Context, r *runtime.RunPodSandboxRequest) (...) {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("cri", "sandbox", "run"),
        tracing.WithNamespace(ctx),
    )
    defer span.End()
```

关键步骤：

**① 生成 sandbox ID**

```go
// sandbox_run.go:64
id := util.GenerateID()  // 32 字节随机数 → 64 字符 hex
```

`util.GenerateID()` 位于 `internal/cri/util/id.go:25`，用 `crypto/rand` 生成 32 字节随机数，hex 编码为 64 字符。

```go
// 文件: internal/cri/util/id.go:25
func GenerateID() string {
    b := make([]byte, 32)
    rand.Read(b)
    return hex.EncodeToString(b)
}
```

**② 创建 metadata 记录**

```go
// sandbox_run.go:161
if _, err := c.client.SandboxStore().Create(ctx, sandboxInfo); err != nil {
```

写入 BoltDB，存储 sandbox 的 labels、spec、runtime 配置等。

**③ 设置 netns + CNI 网络**

```go
// sandbox_run.go:196-278（非 hostNetwork 时）
netns.NewNetNS(sandbox.NetNSPath)                    // 创建网络命名空间
c.client.SandboxStore().Update(ctx, sandboxInfo)      // 更新 metadata
c.setupPodNetwork(ctx, &sandbox)                      // CNI 网络配置
c.client.SandboxStore().Update(ctx, sandboxInfo)      // 更新 metadata
```

**④ 创建 Sandbox (shim)**

```go
// sandbox_run.go:281
c.sandboxService.CreateSandbox(ctx, sandboxInfo, ...)
```

这会启动 shim 进程并创建 sandbox 容器。

**⑤ ensurePauseImageExists**

```go
// sandbox_run.go:298-305
if !ociRuntime.DisablePauseImagePull {
    c.ensurePauseImageExists(ctx, r.GetConfig(), r.GetRuntimeHandler())
}
```

检查 pause 镜像是否存在，不存在则拉取。实现在 `sandbox_run.go:405-427`。

**⑥ 启动 Sandbox**

```go
// sandbox_run.go:307
c.sandboxService.StartSandbox(ctx, sandbox.Sandboxer, id)
```

**⑦ WaitSandbox（独立 trace）**

```go
// sandbox_run.go:385
exitCh, err := c.sandboxService.WaitSandbox(util.NamespacedContext(), sandbox.Sandboxer, id)
```

**这里 trace 被分离**。`util.NamespacedContext()` 定义在 `internal/cri/util/util.go:57`：

```go
func NamespacedContext() context.Context {
    return WithNamespace(context.Background())  // background context，无 parent span
}
```

因为这创建了一个没有任何 span 的 context，`task.Wait` 成为一个新的 root span，有独立的 trace ID。

---

### 3.2 metadata.sandbox.Create

**文件**: `core/metadata/sandbox.go:54`  
**函数**: `(s *sandboxStore) Create()`

```go
ctx, span := tracing.StartSpan(ctx,
    tracing.Name(spanSandboxPrefix, "Create"),
    tracing.WithAttribute("sandbox.id", sandbox.ID),
    tracing.WithNamespace(ctx),
)
```

在 BoltDB 中创建 sandbox 记录：
1. 验证 sandbox ID 格式和时间戳
2. 在 `namespace/<ns>/sandboxes/<id>/` 下创建 bucket
3. 写入 labels、extensions、spec、sandboxer、runtime 配置

**文件**: `core/metadata/sandbox.go:96`  
**函数**: `(s *sandboxStore) Update()`

支持按字段路径局部更新。在 `RunPodSandbox` 中被调用三次：
- `#1`: 更新 `extensions`（含 NetNSPath）
- `#2`: CNI 配置后更新
- `#3`: 更新 `extensions`、`spec`、`labels`（含 selinux_label）

---

### 3.3 cni.setup_pod_network — CNI 网络配置

**文件**: `internal/cri/server/sandbox_run.go:445`  
**函数**: `(c *criService) setupPodNetwork()`

```go
func (c *criService) setupPodNetwork(ctx context.Context, sandbox *sandboxstore.Sandbox) (retErr error) {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("cni", "setup_pod_network"),
        tracing.WithNamespace(ctx),
    )
    defer span.End()
```

流程：
1. `c.getNetworkPlugin(sandbox.RuntimeHandler)` — 获取网络插件（`sandbox_run.go:460`）
2. 可选 `bringUpLoopback()` — 激活 lo 接口（`sandbox_run_linux.go:31`）
3. `cniNamespaceOpts()` — 构建 CNI 参数：labels、port mappings、bandwidth、DNS（`sandbox_run.go:519`）
4. `netPlugin.Setup/SetupSerially()` — 调用 CNI 插件（`sandbox_run.go:490-495`）
5. 从返回结果中提取 IP 地址

**cni.plugin_setup** 子 span 包裹 `netPlugin.Setup()` 调用，该调用进入 go-cni 库：

```
go-cni (vendor/github.com/containerd/go-cni/cni.go):
  └── libcni.Setup() / SetupSerially()
        ├── ready()              // 检查 CNI 就绪状态
        ├── newNamespace()       // 构建 namespace 对象
        ├── attachNetworks()     // 遍历 CNI 网络配置列表
        │   └── Network.Attach()  // 每个 network 调用 CNI 插件
        │       └── CNIConfig.AddNetworkList()  // containernetworking/cni
        │           └── for each plugin in conflist:
        │               └── addNetwork() → ExecPluginWithResult()
        └── createResult()      // 组装结果
```

CNI 插件是独立的二进制程序（如 `/opt/cni/bin/bridge`），通过 stdin 接收 JSON 配置，通过环境变量接收参数：

```
CNI_COMMAND=ADD
CNI_CONTAINERID=<sandbox-id>
CNI_NETNS=/var/run/netns/<xxx>
CNI_IFNAME=eth0
CNI_PATH=/opt/cni/bin
```

---

### 3.4 client.NewContainer

**文件**: `client/container.go`（client 侧）

创建 containerd container 对象。对于 sandbox，这个 container 就是 pause 容器。

```go
// 在 sandbox controller 内部调用 client.NewContainer()
container, err := client.NewContainer(ctx, id, ...)
```

metadata 写入：
- **metadata.containers.Create** — `core/metadata/containers.go:123`，BoltDB 中创建 container 记录

---

### 3.5 container.NewTask — 创建 Task（启动 Shim）

这是整个流程中最复杂的部分，包含 **client → gRPC → server → shim** 四层调用链。

#### 3.5.1 顶层: client/container.go:226

```go
func (c *container) NewTask(ctx context.Context, ...) (_ Task, retErr error) {
    ctx, span := tracing.StartSpan(ctx, "container.NewTask")
    defer span.End()
    // ...
    response, err := c.client.TaskService().Create(ctx, request)  // gRPC 调用
```

#### 3.5.2 gRPC server: plugins/services/tasks/local.go:172

```go
func (l *local) Create(ctx context.Context, r *api.CreateTaskRequest, ...) (...) {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("tasks", "create"))
    defer span.End()
    // ...
    c, err := rtime.Create(ctx, r.ContainerID, opts)  // 委托给 TaskManager
```

#### 3.5.3 TaskManager: core/runtime/v2/task_manager.go:160

```go
func (m *TaskManager) Create(ctx context.Context, taskID string, opts runtime.CreateOpts) (...) {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("runtime", "task_manager", "create"))
    defer span.End()
```

内部子步骤：

| 步骤 | Span | 说明 |
|---|---|---|
| `NewBundle()` | `runtime.bundle.create` | 创建 OCI bundle 目录结构（`bundle.go:47`） |
| `m.mounts.Activate()` | `runtime.mount.activate` | 挂载容器 rootfs |
| `m.manager.Start()` | `shim.manager.start` | 启动 shim 进程 |
| `shimTask.Create()` | `shim.task.create` | RPC 通知 shim 创建容器 |

#### 3.5.4 shim.manager.start — core/runtime/v2/shim_manager.go:199

```go
func (m *ShimManager) Start(ctx context.Context, id string, bundle *Bundle, opts runtime.CreateOpts) (...) {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("shim", "manager", "start"))
    defer span.End()
```

判断是复用已有 sandbox shim 还是启动新的：
- **复用路径**: `loadShim()` — 连接已有 shim
- **启动路径**: `startShim()` → `binary.Start()` — 执行 shim 二进制

#### 3.5.5 shim.binary.exec — core/runtime/v2/binary.go:67

```go
func (b *binary) Start(ctx context.Context, ...) (_ *shim, err error) {
    // ...
    cmd, err := client.Command(ctx, &client.CommandConfig{...})  // 构建 exec.Cmd
    // ...
    out, err = cmd.CombinedOutput()  // ← shim.binary.exec: 执行 shim 进程
    // ...
    conn, err := makeConnection(...) // ← shim.binary.connect: 连接 shim
}
```

`client.Command()` 构建 `exec.Cmd`，指定 shim 二进制路径（如 `containerd-shim-runc-v2`）、bundle 路径、socket 目录等参数。

`cmd.CombinedOutput()` 阻塞直到 shim 进程启动并输出 bootstrap 地址到 stdout。

#### 3.5.6 shim.task.create — core/runtime/v2/shim.go:612

```go
func (s *shimTask) Create(ctx context.Context, opts runtime.CreateOpts) (runtime.Task, error) {
    // ...
    _, err = s.task.Create(ctx, request)  // TTRPC/GRPC 调用 shim
```

这是到 shim 进程的 RPC 调用，shim 收到后实际创建容器（设置 cgroup、namespace 等）。这是 **36.4ms** 的主要消费者。

#### 3.5.7 shim 侧 trace（默认启用）

shim 在 Create 路径上打出与 daemon 相同格式的 `[TRACE]` stderr（`pkg/tracing`），便于 `trace_analyzer.py` 下钻 `shim.task.create` 内部：

```
shim.task.create                         (containerd 侧，等 ttrpc)
└── <otelttrpc> …Create                  (shim server interceptor，若已加载 otelttrpc)
    └── shim.container.create            (task/service.go Create)
        ├── shim.rootfs.mount            (runc/container.go mount.All)
        └── shim.init.create
            └── shim.runc.create         (process/init.go → go-runc Create)
```

编译安装（无需额外 build tag；`-tags shim_tracing` 仍可用，与默认路径等价）：

```bash
go build -o containerd-shim-runc-v2 ./cmd/containerd-shim-runc-v2/
# 安装到 containerd 配置的 runtime binary 路径后重启压测
```

采集时除 `journalctl -u containerd` 外，需合并 shim stderr（shim 的 `[TRACE]` 打在自身进程 stderr）。
---

### 3.6 client.task.Start — 启动 Task

**文件**: `client/task.go:249`

```go
func (t *task) Start(ctx context.Context) error {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("client.task", "Start"), ...)
    defer span.End()
    // ...
    r, err := t.client.TaskService().Start(ctx, &tasks.StartRequest{ContainerID: t.id})
```

调用链：`client.task.Start` → gRPC → `tasks.start` (server) → `shimTask.Start()` → TTRPC → shim

Server 侧：`plugins/services/tasks/local.go:299`

```go
func (l *local) Start(ctx context.Context, r *api.StartRequest, ...) (*api.StartResponse, error) {
    ctx, span := tracing.StartSpan(ctx, tracing.Name("tasks", "start"))
    defer span.End()
    // ...
    p.Start(ctx)  // 委托给 shim
```

---

### 3.7 cri.sandbox.status

**文件**: `internal/cri/server/sandbox_service.go:90`  
**文件**: `internal/cri/server/sandbox_status.go:34`

两次 status 查询在 `RunPodSandbox` 末尾执行，用于确认 sandbox 状态。调用链：

```
cri.sandbox.status
├── cri.sandbox.status.get_ips       // 获取 IP
├── cri.sandbox_service.status       // sandbox service 层
│   └── sandbox.controller.podsandbox.status  // controller 层
└── cri.sandbox.status.to_cri        // 转换为 CRI 格式
```

---

### 3.8 task.Wait — 后台等待（独立 trace）

**文件**: `internal/cri/server/sandbox_run.go:385`

```go
exitCh, err := c.sandboxService.WaitSandbox(util.NamespacedContext(), sandbox.Sandboxer, id)
```

`util.NamespacedContext()` 创建无 parent span 的 context：

```go
// internal/cri/util/util.go:57
func NamespacedContext() context.Context {
    return WithNamespace(context.Background())
}
```

因此 `task.Wait` 成为新 trace 的 root span。它在后台 goroutine 中阻塞等待 shim 进程退出。

---

## 4. 关键耗时分布

| 阶段 | 典型耗时 | 占比 | 瓶颈分析 |
|---|---|---|---|
| CNI 网络配置 | 90ms | 54% | CNI 插件 binary 执行，取决于插件数量和网络环境 |
| 创建 Task（含 shim 启动） | 45ms | 27% | shim binary exec (6ms) + shim RPC (36ms) |
| client.NewContainer | 11ms | 7% | gRPC + metadata 操作 |
| client.task.Start | 5ms | 3% | gRPC + shim RPC |
| metadata 操作 ×4 | ~6ms | 4% | BoltDB 事务 |
| 其他（self time） | 9ms | 5% | pause 镜像检查、NRI 回调等 |

---

## 5. Trace 启用方式

- **containerd daemon**: 无需特殊配置，`EnsureTracerProvider()` 自动初始化
- **shim**: 默认二进制已启用 `EnsureTracerProvider` + otelttrpc plugin；Create 路径含 `shim.container.create` / `shim.rootfs.mount` / `shim.runc.create` 等子 span
- **日志分析**: `python contrib/trace_analyzer.py trace.log`（或 sandbox-tests 的 `scripts/trace_analyzer.py`）；需同时包含 containerd 与 shim 的 `[TRACE]` 行
