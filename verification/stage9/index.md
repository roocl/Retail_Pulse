# 阶段九验收证据

复现命令见[阶段文档](../../docs/stage-9-customer-profiles.md)，自动化运行入口为 [stage9-smoke.ps1](../../scripts/stage9-smoke.ps1)。

| 内容 | 结果入口 |
|---|---|
| 数据来源、观察日期、窗口、规则版本和客户数量 | [batch.json](batch.json) |
| 客户分群、购买与取消金额、订单统计 | [summary.json](summary.json) |
| 示例客户 HTTP 响应 | [sample.json](sample.json) |
| 重复发布、全量分页、筛选、非法参数与数据库重启 | [result.json](result.json) |
| MySQL 实际查询计划 | [query-plan.txt](query-plan.txt) |
| 聚焦测试与用例 | [checks.json](checks.json) |
| 实际浏览器交互 | [browser.json](browser.json) |
| 镜像与交付源码校验 | [runtime.json](runtime.json) |

分群是观察窗口内的描述性规则，不代表预测或实际营销提升。索引计划记录当前数据规模下优化器实际选择，不能推导生产容量。完整日志保留在本机 `target/`，原始工作簿与派生数据不加入 Git。
