# 阶段十验收证据

- [实验清单](manifest.json)：来源、特征版本、时间切分及模型文件校验和。
- [验证集选型](selection.json)与[最终评估](evaluation.json)：生成的指标及代表性排序误差。
- [评分批次](score-batch.json)、[HTTP 样例](prediction.json)、[真实链路断言](result.json)：重复评分和数据集隔离。
- 聚焦测试：`./scripts/offline.ps1 test --include-classname '.*(ExperimentPlanTest|RepurchaseSamplesTest|RepurchaseModelTest|RankingMetricsTest|ProfilesTest|ScoreStoreTest|ProfileStoreTest)'`，8 项通过，无跳过；客户数据库启用时 `CustomerHttpTest` 1 项通过，无跳过。
- 浏览器验收：客户详情显示历史画像和独立预测区，模型来源、观察日、分数与 HTTP 一致；窄窗口可滚动阅读，明确显示未经概率校准。

原始日志保留在本机 `target/stage10-*`。完整模型、Parquet 样本及逐条测试评分保留在被忽略的 `customer-analytics/data/retail/models/`；按实验清单校验后读取。`samples` 查询已保存样本的时点与标签计数，`evaluate` 读取已保存报告，均不重新选择模型。

本次仅证明历史既有客户的离线排序与本机服务链路；未验证真实营销收益、现代客户分布、集群吞吐或概率校准。测试客户可跨时点出现，样本数不是去重客户数。
