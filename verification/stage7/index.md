# 阶段七验证证据

运行入口与边界见[阶段七运行文档](../../docs/stage-7-operations.md)。以下文件保存实际检查结果，数值以 JSON 为准。

| 检查 | 入口 | 证据 |
|---|---|---|
| 契约、解析、作业配置与查询异常 | 聚焦 Maven 命令见结果文件 | [checks.json](checks.json) |
| 四类服务恢复与重复收敛 | [stage7-smoke.ps1](../../scripts/stage7-smoke.ps1) | [结果](recovery/results.json)、[配置](recovery/settings.json)，同目录保存输入和恢复前后检查点 |
| 空数据卷初始化与查询 | [stage7-clean-start.ps1](../../scripts/stage7-clean-start.ps1) | [结果](clean-start/result.json) |
| 正式生产者、Savepoint 停止恢复与重复启动 | [stage7-lifecycle.ps1](../../scripts/stage7-lifecycle.ps1) | [结果](lifecycle/result.json)、[恢复状态](lifecycle/checkpoints.json) |
| 批次性能 | [stage7-benchmark.ps1](../../scripts/stage7-benchmark.ps1) | [环境](benchmark/environment.json)、[汇总](benchmark/summary.json)，同目录保存每次测量 |
| 并行空闲输入边界 | 命令见场景文件 | [场景](parallel-idle/scenario.json)、[实际输出](parallel-idle/output.txt) |

基准使用预热后独立批次、默认并行度和单活跃 Kafka 分区。耗时包含 Kafka CLI 启动、输入传输、发送确认与 API 可见等待；资源为采样时快照。该结果不代表持续容量、峰值内存、逐事件延迟或生产 SLA。

完整输入和运行日志位于执行命令产生的本机 `target/` 目录；提交的证据保留参数、原始测量及输入校验和。不同运行使用隔离的数据集和事件标识，不能将其结果合并作为一次实验。
