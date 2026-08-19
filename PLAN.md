# Database CDC MCP 执行计划

## 1. 目标

构建一个面向桌面用户的本地 MCP Server。第一版仅支持 MySQL，在指定的一段时间内监听启动后产生的 `INSERT`、`UPDATE`、`DELETE` 事件，并允许 MCP 客户端异步拉取结果。

发布物使用 .NET self-contained 模式，最终用户不需要预装 .NET 运行时。

## 2. MVP 范围

- 单进程、单 MySQL 数据源。
- 使用 MCP `stdio` 传输，由桌面 MCP 客户端按需启动。
- MySQL 连接信息由环境变量或本地配置提供，不允许通过 MCP tool 传入密码。
- 支持按数据库、表和操作类型过滤。
- 每个监听会话设置持续时间和最大事务数。
- 事件保存在内存中，服务重启后不恢复。
- 默认从当前 Binlog 末尾开始，只返回监听启动后的变化。
- 同一时间最多运行 32 个逻辑监听会话，共享一条 MySQL Binlog 复制流和一个 replication `server_id`。
- 单个会话最长 1 小时、默认监听 10 分钟、完整事务数量上限最多为 100,000。
- 内部限制单事务和单监听累计行变化数量；达到保护上限时拒绝整个事务，不保存部分事务。

## 3. 非目标

- 监听不自动做初始全量快照；现有表数据通过独立的只读查询 Tool 按页读取。
- 不保证 exactly-once，不提供服务重启后的断点续传。
- 不实现 Kafka、多节点高可用或分布式订阅。
- 不处理 DDL 事件。
- 第一版不提供桌面 GUI，先交付可直接被 MCP 客户端启动的本地可执行程序。

## 4. 技术选型

- .NET 10
- ModelContextProtocol 2.2.0
- MySqlCdc 4.0.1
- MySqlConnector 2.6.2
- Microsoft.Extensions.Hosting 10.0.11
- Windows `win-x64` self-contained 发布

## 5. MCP Tools

### `start_mysql_watch`

启动一个监听会话，参数包括数据库、表、操作类型、持续秒数和完整事务数量上限，返回 `watchId`。

### `get_mysql_watch_events`

按 `watchId` 和事务序号增量读取完整事务，返回当前状态及下一次读取游标。

### `get_mysql_watch_status`

读取会话状态、已捕获事务数、行变化数、开始时间、结束时间和错误信息。

### `get_mysql_watch_targets`

读取当前正在运行的监听目标，包括数据库、表过滤条件和操作过滤条件。

### `stop_mysql_watch`

主动停止一个逻辑监听，不影响其他监听共享的 Binlog 连接。

### `get_mysql_table_schema`

只读查询指定 MySQL 表的列结构和元数据。

### `get_mysql_table_data`

只读分页查询指定 MySQL 表的数据，限制单次返回行数并使用安全引用的表标识符。

## 6. 执行阶段

- [x] 确认桌面优先、短时监听的 MVP 边界。
- [x] 核实 MCP C# SDK 与 MySqlCdc 包版本和基础 API。
- [x] 写入执行计划。
- [x] 搭建 .NET 解决方案、配置和 MCP stdio 服务。
- [x] 实现监听会话管理、内存事件队列和边界校验。
- [x] 接入 MySqlCdc 并标准化行事件。
- [x] 添加单元测试和可替换的变更流抽象。
- [x] 零警告编译并通过 stdio 验证 MCP 初始化、tools/list 和 tool error。
- [x] 还原测试依赖并运行 `dotnet test`。
- [x] 添加 MySQL 本地验证说明和示例 MCP 配置。
- [ ] 使用真实 MySQL 验证增删改 Binlog（当前机器没有 MySQL 或 Docker）。
- [x] 生成 Windows self-contained 发布物。

当前发布物位于 `artifacts/win-x64/DatabaseCdcMcp.exe`，约 75 MB。

## 7. 验收标准

1. MCP 客户端可以通过 stdio 启动服务并列出五个监听 tools。
2. 启动监听后，对目标表执行增删改能够按提交事务读取对应事件。
3. 非目标数据库、表和操作不会进入会话。
4. 达到持续时间、事务数量上限或内部内存保护上限后会话自动结束。
5. 主动停止和客户端取消不会导致后台任务遗留。
6. 密码不会出现在 MCP tool 参数、返回值或普通日志中。
7. `dotnet test` 通过，并能生成无需安装 .NET 的 Windows 可执行程序。

## 8. MySQL 前置条件

```ini
binlog_format=ROW
binlog_row_image=FULL
```

监听账号需要：

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON your_database.* TO 'cdc_user'@'%';
```

MySqlCdc 4.0.1 当前未完全支持 SSL。第一版仅建议用于可信本地网络或开发环境；远程数据库场景需要在后续版本中替换或加固连接层。
