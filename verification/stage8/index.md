# 阶段八验证证据

运行方式见[阶段八文档](../../docs/stage-8-retail-warehouse.md)，可复现入口为 [stage8-smoke.ps1](../../scripts/stage8-smoke.ps1)。

| 内容 | 唯一结果入口 |
|---|---|
| 聚焦测试、打包与 CLI | [checks.json](checks.json) |
| 原始来源、校验和、数量及缺失分布 | [quality.json](quality.json) 的 `source` 与 `input_profile` |
| 原始与清洗行数、金额、记录去向和维度关联 | [quality.json](quality.json) |
| 重复接入、重复构建与 MySQL/Hive 重启 | [result.json](result.json) |
| 发布数据库与转换版本 | [manifest.json](manifest.json) |
| 实际 Java、Spark 及应用校验 | [runtime.json](runtime.json) |
| 基准发布的质量指标与双向逐行差集 | [equivalence.json](equivalence.json)、[比较 SQL](equivalence.sql) |
| 月份筛选的物理计划与分区裁剪 | [partition-plan.txt](partition-plan.txt) |

真实数据文件与完整运行日志保留在本机被忽略的 `customer-analytics/data/` 和 `target/`。清洗报告的各去向互斥；输入质量特征允许重叠，不能将缺失、取消、负数量与异常价格的计数直接相加。

取消金额表示独立取消记录的带符号金额，不代表已匹配原订单的退款。实验采用单机 Spark、共享 Parquet 卷和独立 Hive Metastore/MySQL，不证明分布式容量或高可用。
