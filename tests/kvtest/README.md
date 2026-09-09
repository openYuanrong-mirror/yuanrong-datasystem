# kvtest

独立的 datasystem KVClient 性能测试工具，支持 Writer/Reader 角色分离、Cache 模式、Benchmark Set/Get 模式、多节点部署、K8s 自动发现。同时提供 Worker/Coordinator 独立部署测试程序（coordinator_test / worker_test）和外部服务发现模拟（mock_jf_server.py），用于验证独立集成部署与服务发现对接流程。

## 编译与运行

```bash
cd tests/kvtest

# 编译（默认 Bazel：in-tree datasystem，自包含二进制，无需预装 SDK；
#       自动启用 KVTEST_USE_BRPC ---- brpc 控制面 + bthread pipeline/notify 池）
./build.sh

# 或指定 SDK 路径
./build.sh -s /path/to/sdk

# 用 Bazel 构建并将 URMA 支持编入自包含 kvtest（与根目录 build.sh -M on 对齐）
./build.sh -b bazel -M on

# Debug 构建
./build.sh -d                # bazel: --config=debug
./build.sh -b bazel -d

# 用 CMake + brpc 后端构建（默认 KVTEST_USE_BRPC=ON，行为与 bazel 一致；
#       复用主仓 cmake/external_libs/*.cmake 自动下载编译 brpc/protobuf/gflags/absl，
#       缓存到 $DS_OPENSOURCE_DIR，首次 5-10 分钟，之后秒级；仍链预装 libdatasystem.so）
./build.sh -b cmake
./build.sh -b cmake -s /path/to/sdk

# CMake + httplib 后端（fallback：无第三方依赖，无网络下载，纯 std::thread）
./build.sh -b cmake --use-httplib
```

Bazel 模式不会在运行时加载外部 `libdatasystem.so`。需要 UB/URMA 数据面时，必须在构建 kvtest 本身时传入
`-M on`；该选项会映射为 Bazel 的 `--config=urma`。默认值为 `off`。CMake 模式的能力由 `-s` 指定的
预构建 SDK 决定，因此不接受 `-M on`。

> cmake+brpc 模式下第三方件源码默认从 gitee/github 下载。如需离线/加速，可设
> `export DS_LOCAL_LIBS_DIR=/path/to/opensource_third_party` 指向主仓

## 运行

### Jemalloc profiling（Bazel）

```bash
# 编译支持，默认 -x off；CMake 不接受 -x on
./build.sh -b bazel -x on
./output/kvtest --version
# jemalloc_prof_supported=true

# 部署时独立开启采样及周期性 dump
python3 deploy_client.py deploy config/deploy.json config/config.json \
  --jemalloc_prof_conf 'lg_prof_sample:19,lg_prof_interval:30,prof_final:true'
```

Bazel 构建的 `kvtest`、`coordinator_test`、`worker_test` 默认链接普通版共享 jemalloc；
`-x on` 复用 `--config=jeprof`，同时将三个工具切换到 profiling 版。
两种构建都将 `libjemalloc.so.2` 打包到 `output/lib/`。部署时应保留二进制旁的 `lib/` 目录；
客户端安装将该库单独上传到受管的 `allocator_lib/`，每次在 host lock 内重建该目录。
即使指定 `remote_sdk_dir`，也只将 `allocator_lib/` 置于指定 SDK 之前，不提升历史 `lib/`
中其他 SDK/URMA 库的优先级。该受管目录只允许存放配套 jemalloc，不应放入用户文件。
普通启动和 `--version` 都打印
`jemalloc_prof_supported=true/false`，两种 Bazel 构建都通过实际加载库的 `mallctl("config.prof")` 查询能力。
编译支持并不自动开启采样。

独立启动 `coordinator_test` 或 `worker_test` 时，在进程启动前设置 `MALLOC_CONF`：

```bash
cd tests/kvtest
bash build.sh -b bazel -M off -x on
mkdir -p "$PWD/output/logs/jemalloc"
MALLOC_CONF="prof:true,lg_prof_sample:19,prof_final:true,prof_prefix:$PWD/output/logs/jemalloc/worker_test" \
  ./output/worker_test --version
```

上述命令用正常退出的 `--version` 验证能力及最终 dump；业务运行时替换为实际启动参数。
`coordinator_test` 用法相同，设置独立的 `prof_prefix`。运行时目录应使用实际日志目录下的
`jemalloc/`，并提前创建。通过部署脚本启动时，使用下面的统一参数自动准备目录与环境变量：

```bash
python3 deploy_worker.py start -p worker -c worker.config -S --jf jf:31500 \
  --jemalloc_prof_conf 'lg_prof_sample:19,lg_prof_interval:30,prof_final:true'
python3 deploy_coordinator.py start -p coordinator -c coordinator.config -S --jf jf:31500 \
  --jemalloc_prof_conf 'lg_prof_sample:19,lg_prof_interval:30,prof_final:true'
```

两者的 `deploy` 子命令也支持该参数，安装阶段应提供 `-x on` 编译的二进制及配套动态库。
日志目录取应用 `--set` 后配置中的 `log_dir`（支持字符串及 `{"value": ...}` 格式），默认前缀为：

| 工具 | 默认 `prof_prefix` |
|---|---|
| `worker_test` | `<log_dir>/jemalloc/worker_test_<port>` |
| `coordinator_test` | `<log_dir>/jemalloc/coordinator_test_<port>` |

例如 `log_dir=/path/to/log`、Worker 端口为 31501，则前缀为
`/path/to/log/jemalloc/worker_test_31501`。未配置 `log_dir` 时必须显式指定 `prof_prefix`。
相对路径在目标进程工作目录下解析；在远端检查能力、创建并检查目录后，才通过 launcher 或
nohup 启动。缺少 profiling 能力或目录不可访问时启动失败，不会静默忽略配置。

Worker 的旧参数 `--jemalloc-prof-options` 保留为别名，支持 standalone 与 dscli 两种模式。
dscli Worker 仍由 dscli 自身准备 profiling 环境。Coordinator 的该参数仅支持 `-S`：
底层 dscli 目前只支持 Worker profiling，因此 Coordinator 非 standalone 模式会明确拒绝该参数。

分步部署时，`install` 上传二进制和动态库，`start --jemalloc_prof_conf ...` 在启动前设置采样配置；
`deploy` 则依次执行安装和启动，任一阶段失败都会返回非零状态。

`--jemalloc_prof_conf` 按 jemalloc 的 `MALLOC_CONF` 格式接收配置，拒绝空配置、格式错误、
重复键和 `prof:false`、`prof_active:false`、`prof_thread_active_init:false`，避免关闭采样。
缺省补充 `prof:true`，已知 profiling 布尔项只接受 true/false；
`lg_prof_sample` 接受 0–63，`lg_prof_interval` 接受 -1–63（-1 表示不做周期 dump）。
其余采样和 dump 策略由用户指定。
该参数覆盖实例 `env` 中的 `MALLOC_CONF`。未指定参数时保持原有环境变量行为。

默认 profile 目录为 **kvtest 日志目录 `output_dir` 下的 `jemalloc/`**：

| 配置 | 路径 |
|---|---|
| `output_dir` | `/path/to/log` |
| profile 目录 | `/path/to/log/jemalloc/` |
| 默认 `prof_prefix`，实例 ID 为 3 | `/path/to/log/jemalloc/kvtest_3` |

未配置 `output_dir` 且传入 profiling 参数时，部署端生成 `metrics_<instance_id>_<时间戳>`，
写入上传的实例配置，保证日志与 profile 使用同一目录。相对路径以远端工作目录为基准，
不是部署机的当前目录。显式 `prof_prefix` 优先，且必须包含目录。
启动前会检查远端二进制支持情况，并在远端主机或目标容器内创建和检查 profile 父目录。
能力不足或目录不可写时，该实例部署失败，部署命令返回非零。
显式传入 profiling 配置时，若发现工具进程已运行，命令会失败并提示先停止再启动，不会把
“已有进程”报告为配置生效。服务工具单 Pod 的预检失败会计入该 Pod 的失败结果，
其余 Pod 继续完成，最后汇总成功数并返回非零。

profile 文件名由 jemalloc 在前缀后追加 PID、序号和 dump 类型。
仅打开 `prof:true` 不保证立即生成文件；上例配置周期性 dump 和正常退出 dump，
强制终止进程不能依赖退出 dump。profiling 会影响压测吞吐、时延和磁盘占用，应按需开启，
并由使用者清理采集文件；关闭采样需移除部署参数及自行设置的 `MALLOC_CONF`。
这些 profile 主要覆盖普通堆分配，不代表全部 RSS，也不覆盖独立的 `datasystem_*` 分配器。

回归验证（仓库根目录）：

```bash
bazel test //tests/kvtest:jemalloc_prof_test --config=jeprof --test_output=errors
bazel test //tests/kvtest:jemalloc_prof_test --define=enable_jemalloc_prof=false --test_output=errors
```

测试会复制二进制到独立目录，验证运行库加载、能力输出及 malloc/free 的实际提供者。
使用 glibc 的 `LD_DEBUG=bindings` 和立即绑定检查二进制的符号解析结果必须指向随包 jemalloc，
而不是仅检查 ELF NEEDED。在支持版生成实际 heap 文件。三个工具各有对应的测试目标；
Python 反例测试还构造仅提供 mallctl、不接管 malloc/free 的 DSO，确认断言会拒绝该错误链接。

### 分配器基线版本与回退

本 PR 是压测工具的分配器基线切换，不是与历史结果可直接混用的诊断开关。

| 基线标识 | 范围 | 比较规则 |
|---|---|---|
| `kvtest-bazel-allocator-v0` | 本 PR 之前的已归档二进制、库和构建提交；参考基点 `a0b1a39ec` | 保持原有工具及依赖；实际分配器以归档的符号绑定记录为准 |
| `kvtest-bazel-jemalloc-v1` | 本 PR 起，三个 Bazel 工具默认共享 jemalloc，`-x off` | 建立新的吞吐、时延、CPU、RSS 基线，不直接与 v0 合并 |
| `kvtest-bazel-jemalloc-prof-v1` | 同一源码的 `-x on` 及显式采样策略 | 单独标记采样配置，不用作无采样性能基线 |

每份报告应记录基线标识、三个工具的完整构建 SHA、构建后端、`--version` 输出、二进制/运行库
校验值、malloc/free 提供者、`MALLOC_CONF`、服务端版本和 CPU/NUMA 绑定。
不能仅凭工具 VERSION 相同就认定结果可比。CMake/SDK 构建单独记录，不归入上述 Bazel 基线。
`-x off` 的普通 jemalloc 不具备 profiling 能力、没有 profiling 采样开销，但仍会改变
分配延迟、线程缓存、碎片和 RSS；它不是回退至 libc 的开关。

尚未获得代表性性能对照数据，也没有据此承诺性能回归接受阈值。需要跨 v0/v1 比较时，
固定服务端版本、CPU/NUMA 和负载参数，对短对象高 QPS/较大对象、单实例/多实例各运行多轮，
记录吞吐、p50/p99、CPU、RSS/碎片及方差，由压测负责人确认接受阈值后再判断回归。
若新基线不能接受，停止新工具，在独立工作目录恢复已归档的 v0 二进制及完整依赖并重新启动，
同时取消 profiling 参数和相关环境变量；不要只替换同名 jemalloc DSO 或混用新旧工具包。

### 常规运行

```bash
# 启动依赖
etcd &
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# 运行
cd tests/kvtest/output
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/my_config.json

# 查看统计 / 停止（HTTP 端点路径不变：/stats、/stop、/summary、/notify）
# bazel 构建：brpc 经 restful 映射保留旧路径，响应 /stats 为 {"stats_json":"<metrics json>"}
curl -s http://127.0.0.1:9000/stats | python3 -m json.tool
curl -X POST http://127.0.0.1:9000/stop
# cmake 构建仍走 httplib：/stats 直接返回 metrics JSON
```

## Benchmark Set/Get 模式

用于精确测量 Set/Get 吞吐和延迟，支持 8 种测试模式：

```bash
# 本地 Set 吞吐基线（8线程，5轮）
cat > config/bench.json << 'EOF'
{
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "test_mode": "set_local",
  "worker_memory_mb": 4096,
  "num_threads": 8,
  "total_rounds": 5,
  "data_sizes": ["8MB"],
  "set_api": "string_view",
  "cleanup_method": "del"
}
EOF

LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/bench.json
```

**测试模式：** `set_local` / `set_remote` / `get_local` / `get_cross_node` / `get_remote_direct` / `get_remote_cross` / `mixed_local` / `mixed_cross_node`

**Set API：** `string_view`（直接写入）/ `create_buffer`（SHM Buffer + latch）/ `create_buffer_raw`（SHM Buffer，无锁 memcpy）

**输出：** `benchmark_phases.csv`（per-round per-phase 延迟和 QPS）

## 测试

```bash
# C++ 单元测试 (68) + Python 单元测试 (53)
cd tests/kvtest
bash tests/run_all_tests.sh

# 集成测试（需要真实集群环境）
bash tests/test_cpu_affinity.sh   # CPU 绑核验证
bash tests/test_deploy.sh          # 多节点部署验证
bash tests/test_e2e.sh             # 端到端验收测试

# Worker/Coordinator 独立部署 + 服务发现模拟测试
bash tests/test_standalone_mode.sh
# 覆盖场景：Coordinator 注册/心跳/反注册、Worker 从服务发现获取 Coordinator、
#           Coordinator 崩溃 + TTL 过期、Coordinator 重启恢复
```

## 文档

| 文档 | 内容 |
|------|------|
| [docs/user-guide.md](docs/user-guide.md) | 编译部署、配置参数、远程部署、指标采集、故障排查 |
| [docs/pipeline-guide.md](docs/pipeline-guide.md) | Pipeline 模式：Writer/Reader 角色、QPS 控制、多实例部署 |
| [docs/cache-guide.md](docs/cache-guide.md) | Cache 模式：cacheGetOrCreate、命中率控制、Key Pool 管理 |
| [docs/benchmark-guide.md](docs/benchmark-guide.md) | Benchmark 模式：8 种 Set/Get/Mixed 测试模式、per-phase 计时 |
| [docs/design.md](docs/design.md) | 架构设计：模块设计、线程模型、指标系统、QPS 控制机制 |
| [docs/jf-integration-design.md](docs/jf-integration-design.md) | 独立部署 + 服务发现模拟：JfClient、mock server、deploy 脚本 standalone 模式、E2E 测试 |
