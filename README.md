# Database CDC MCP

一个面向桌面 MCP 客户端的本地 MySQL 数据变化监听服务。它从当前 Binlog 末尾开始，在限定时间内收集目标数据库表的 `INSERT`、`UPDATE` 和 `DELETE` 事件，再由 MCP 客户端读取这些事件。

本文是一份从零开始的使用说明。按照“准备 MySQL -> 发布程序 -> 配置桌面 MCP 客户端 -> 启动监听 -> 读取事件”的顺序执行即可。

## 1. 使用边界

- 当前只支持 MySQL。
- 传输方式是 MCP `stdio`，桌面 MCP 客户端会按配置启动本地程序。
- 监听从启动时的 Binlog 末尾开始，不读取历史数据。
- 事件只保存在内存中，程序重启后不会恢复。
- 同一时间最多运行 1 个监听会话。
- 单次监听时间范围为 1 到 1800 秒（最长 30 分钟）。
- 单个会话最多保留 100000 条事件。
- 单次读取最多返回 1000 条事件。
- 监听不会自动提供初始全量快照；如需读取现有数据，可调用表数据查询工具。当前也不监听 DDL 变化。

## 2. 准备环境

### 使用已发布程序

普通使用者只需要 Windows 和 MySQL，不需要预装 .NET。发布产物是 Windows `win-x64` self-contained 单文件程序。

### 从源代码运行或重新发布

开发者需要安装 .NET SDK 10。项目使用的 SDK 版本见 [global.json](global.json)。在仓库根目录执行：

```powershell
dotnet --version
dotnet restore src\DatabaseCdcMcp\DatabaseCdcMcp.csproj
dotnet restore tests\DatabaseCdcMcp.Tests\DatabaseCdcMcp.Tests.csproj
dotnet build src\DatabaseCdcMcp\DatabaseCdcMcp.csproj --no-restore
dotnet test tests\DatabaseCdcMcp.Tests\DatabaseCdcMcp.Tests.csproj --no-restore
```

## 3. 准备 MySQL 5.7

本项目可以连接 MySQL 5.7。你现有的其他字符集、连接数和 InnoDB 参数可以继续保留，CDC 相关的关键配置只有下面几项。

### 3.1 开启 ROW Binlog

MySQL 必须使用行级 Binlog，并记录完整行数据：

```ini
binlog_format=ROW
binlog_row_image=FULL
```

一个适用于 MySQL 5.7 的最小配置示例：

```ini
[mysqld]
port=3306
server_id=1
log-bin=binlog
binlog_format=ROW
binlog_row_image=FULL

# Binlog 自动保留 7 天；按磁盘空间和业务需要调整
expire_logs_days=7
```

如果你的配置中已经有：

```ini
binlog_format=mixed
```

请改为 `binlog_format=ROW`。本项目读取的是 `WriteRowsEvent`、`UpdateRowsEvent` 和 `DeleteRowsEvent`；使用 `MIXED` 时，部分 SQL 可能按语句记录，无法稳定转换为行变化事件。

`server_id=1` 是 MySQL 服务本身的复制 ID，可以保留。MCP 连接使用另一个 ID，例如：

```text
MYSQL_CDC_SERVER_ID=6174
```

MySQL 服务端和复制客户端不能使用相同的 `server_id`。

`expire_logs_days=7` 表示自动保留 Binlog 约 7 天。设置为 `0` 表示不自动过期。MCP 当前从 Binlog 末尾开始监听，不需要依赖历史 Binlog，但生产环境仍应根据磁盘空间设置合理保留时间。

可以先检查当前配置：

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
SHOW VARIABLES LIKE 'expire_logs_days';
SHOW VARIABLES LIKE 'server_id';
SHOW MASTER STATUS;
--SELECT @@global.log_bin, @@global.binlog_format, @@global.binlog_row_image, @@global.expire_logs_days;
```

预期结果：

- `log_bin` 为 `ON`
- `binlog_format` 为 `ROW`
- `binlog_row_image` 为 `FULL`
- `server_id` 为非零值，并且与 MCP 使用的 `MYSQL_CDC_SERVER_ID` 不同
- `expire_logs_days` 为你期望的保留天数

如果配置不正确，请在 MySQL 配置文件的 `[mysqld]` 节中加入上面的配置并重启 MySQL。具体配置文件位置取决于你的安装方式。

也可以临时修改 Binlog 过期天数：

```sql
SET GLOBAL expire_logs_days = 7;
```

临时修改重启后可能失效，长期配置仍应写入 MySQL 配置文件。

### 3.2 创建监听账号

先确认数据库版本：

```sql
SELECT VERSION();
```

将下面的 `your_database` 和密码替换成实际值。账号中的 `%` 表示允许任意主机连接；如果 MCP 和 MySQL 在同一台机器，可以将其改为 `127.0.0.1`，进一步限制连接来源。

#### MySQL 5.7

MySQL 5.7 使用 `REPLICATION SLAVE` 权限名称：

```sql
CREATE USER IF NOT EXISTS 'cdc_user'@'%'
IDENTIFIED BY 'asdfjj';

GRANT REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'cdc_user'@'%';

GRANT SELECT
ON `your_database`.* TO 'cdc_user'@'%';
-- 设置全部表的查询权限    
-- GRANT SELECT
-- ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
-- 查询权限设置情况
SHOW GRANTS FOR 'cdc_user'@'%';
```

#### MySQL 8.0.23 及以上

MySQL 8.0.23 引入了更准确的权限名称 `REPLICATION REPLICA`：

```sql
CREATE USER IF NOT EXISTS 'cdc_user'@'%'
IDENTIFIED BY 'replace-with-a-strong-password';

GRANT REPLICATION REPLICA, REPLICATION CLIENT
ON *.* TO 'cdc_user'@'%';

GRANT SELECT
ON `your_database`.* TO 'cdc_user'@'%';

FLUSH PRIVILEGES;
```

#### MySQL 8.0.22 及以下

如果是较早的 MySQL 8.0 版本，使用兼容权限名称：

```sql
CREATE USER IF NOT EXISTS 'cdc_user'@'%'
IDENTIFIED BY 'replace-with-a-strong-password';

GRANT REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'cdc_user'@'%';

GRANT SELECT
ON `your_database`.* TO 'cdc_user'@'%';

FLUSH PRIVILEGES;
```

MySQL 8.0.23 及以上通常仍兼容 `REPLICATION SLAVE`，但新建配置建议使用 `REPLICATION REPLICA`。如果执行 `REPLICATION REPLICA` 时提示权限名称不存在，请改用 `REPLICATION SLAVE`。

如果账号已经存在，第一条 `CREATE USER` 会报账号已存在，此时改用：

```sql
ALTER USER 'cdc_user'@'%'
IDENTIFIED BY 'replace-with-a-strong-password';
```

这些权限的用途是：

- `REPLICATION SLAVE`（MySQL 5.7 和早期 MySQL 8.0）或 `REPLICATION REPLICA`（MySQL 8.0.23+）：读取 Binlog 行事件。
- `REPLICATION CLIENT`：读取复制状态和 Binlog 信息。
- 目标数据库的 `SELECT`：读取 `INFORMATION_SCHEMA.COLUMNS`，将 Binlog 中按位置排列的值映射为列名。

如果只允许固定来源连接，可以把账号中的 `%` 换成 MCP 程序所在机器的 IP 地址，例如 `'cdc_user'@'127.0.0.1'`。

授权完成后检查：

```sql
SHOW GRANTS FOR 'cdc_user'@'%';
```

### 3.3 注意 SSL

当前 `MySqlCdc 4.0.1` 的 Binlog 连接在本项目中显式关闭 SSL。请只在可信的本地网络或开发环境使用，不要直接暴露到不可信网络。

## 4. 发布 MCP 程序

在仓库根目录执行：

```powershell
dotnet publish src\DatabaseCdcMcp\DatabaseCdcMcp.csproj `
  -c Release `
  -r win-x64 `
  --self-contained true `
  -o artifacts\win-x64
```

发布完成后，确认文件存在：

```powershell
Test-Path .\artifacts\win-x64\DatabaseCdcMcp.exe
```

结果为 `True` 即可。MCP 客户端配置中的 `command` 必须填写这个 `.exe` 的绝对路径，例如：

```text
D:\desktop\DatabaseCdcMcp\artifacts\win-x64\DatabaseCdcMcp.exe
```

不要直接双击程序。它使用 `stdio` 与 MCP 客户端通信，需要由 MCP 客户端启动并保持标准输入输出连接。

## 5. 配置桌面 MCP 客户端

不同桌面客户端的配置文件位置和界面名称可能不同，但配置内容都需要包含：

- `command`：`DatabaseCdcMcp.exe` 的绝对路径。
- `env`：MySQL 连接环境变量。

通用 JSON 配置如下：

```json
{
  "mcpServers": {
    "database-cdc": {
      "command": "D:\\desktop\\DatabaseCdcMcp\\artifacts\\win-x64\\DatabaseCdcMcp.exe",
      "env": {
        "MYSQL_CDC_HOST": "127.0.0.1",
        "MYSQL_CDC_PORT": "3306",
        "MYSQL_CDC_USER": "cdc_user",
        "MYSQL_CDC_PASSWORD": "replace-with-a-strong-password",
        "MYSQL_CDC_SERVER_ID": "6174"
      }
    }
  }
}
```

在 JSON 中 Windows 反斜杠必须写成 `\\`。`MYSQL_CDC_SERVER_ID` 是复制客户端 ID，同一个 MySQL 实例上不要让多个复制客户端使用相同的 ID。

保存配置后，完全退出并重新打开桌面 MCP 客户端，使它重新启动 MCP Server。连接成功后，客户端应该能发现以下六个工具：

```text
start_mysql_watch
get_mysql_watch_events
get_mysql_watch_status
get_mysql_watch_targets
stop_mysql_watch
get_mysql_table_schema
get_mysql_table_data
```

### 配置 Codex 使用此 MCP

如果使用的是 Codex，可以直接在 Codex 的 MCP 配置中添加这个本地 `stdio` Server。Windows 默认配置文件为：

```text
%USERPROFILE%\.codex\config.toml
```

例如当前用户通常对应：

```text
C:\Users\Administrator\.codex\config.toml
```

先按照第 4 节发布程序，然后在 `config.toml` 末尾加入：

```toml
[mcp_servers.database_cdc]
type = "stdio"
command = 'D:\desktop\DatabaseCdcMcp\artifacts\win-x64\DatabaseCdcMcp.exe'
args = []
startup_timeout_sec = 30

[mcp_servers.database_cdc.env]
MYSQL_CDC_HOST = "127.0.0.1"
MYSQL_CDC_PORT = "3306"
MYSQL_CDC_USER = "cdc_user"
MYSQL_CDC_PASSWORD = "replace-with-a-strong-password"
MYSQL_CDC_SERVER_ID = "6174"
```

将 `command` 改为实际的 `DatabaseCdcMcp.exe` 绝对路径，将账号和密码改为实际值。`MYSQL_CDC_SERVER_ID` 必须与 MySQL 的 `server_id` 不同，例如 MySQL 使用 `server_id=1` 时，MCP 可以使用 `6174`。

保存后完全退出并重新打开 Codex，或创建一个新的 Codex 任务。MCP 工具通常在任务启动时加载，已经运行的任务不会自动出现新工具。连接成功后，应能看到：

```text
start_mysql_watch
get_mysql_watch_events
get_mysql_watch_status
get_mysql_watch_targets
stop_mysql_watch
get_mysql_table_schema
get_mysql_table_data
```

配置文件中会保存数据库密码明文，请使用权限受限的 CDC 专用账号，并限制配置文件的访问权限。

### 从源代码启动（开发者可选）

如果暂时不发布，也可以将 MCP 配置中的启动命令改为：

```json
{
  "command": "dotnet",
  "args": [
    "run",
    "--project",
    "D:\\desktop\\DatabaseCdcMcp\\src\\DatabaseCdcMcp\\DatabaseCdcMcp.csproj",
    "--no-build"
  ],
  "env": {
    "MYSQL_CDC_HOST": "127.0.0.1",
    "MYSQL_CDC_PORT": "3306",
    "MYSQL_CDC_USER": "cdc_user",
    "MYSQL_CDC_PASSWORD": "replace-with-a-strong-password",
    "MYSQL_CDC_SERVER_ID": "6174"
  }
}
```

这种方式要求本机已安装对应的 .NET SDK，并且已经执行过构建。

## 6. 完整使用流程

### 第一步：启动监听

可以直接告诉 MCP 客户端：

```text
请监听 demo 数据库的 orders 表 120 秒，只监听 insert 和 update，最多保留 100 条事件。
```

客户端应调用 `start_mysql_watch`，对应参数如下：

```json
{
  "database": "demo",
  "tables": ["orders"],
  "operations": ["insert", "update"],
  "durationSeconds": 120,
  "maxEvents": 100
}
```

成功后会返回类似结果：

```json
{
  "watchId": "a1b2c3d4...",
  "state": "starting",
  "startedAt": "2026-08-17T10:00:00+00:00",
  "expiresAt": "2026-08-17T10:02:00+00:00",
  "maxEvents": 100
}
```

记住返回的 `watchId`，后续查询和停止都需要使用它。

如果 `tables` 为空或省略，则监听该数据库中的所有表；如果 `operations` 为空或省略，则监听 `insert`、`update` 和 `delete` 全部操作。

### 第二步：查询当前监听目标

调用 `get_mysql_watch_targets` 时不需要传入参数。它只返回当前处于 `starting` 或 `running` 状态的监听：

```json
{}
```

返回结果类似：

```json
{
  "watches": [
    {
      "watchId": "a1b2c3d4...",
      "state": "running",
      "database": "demo",
      "allTables": false,
      "tables": ["orders"],
      "operations": ["insert", "update"],
      "startedAt": "2026-08-17T10:00:00+00:00",
      "expiresAt": "2026-08-17T10:02:00+00:00"
    }
  ]
}
```

`allTables` 为 `true` 时表示监听该数据库的所有表，此时 `tables` 会是空数组。`watches` 为空数组表示当前没有活动监听。

### 第三步：在监听期间修改 MySQL 数据

必须在监听启动之后执行 SQL。例如：

```sql
INSERT INTO demo.orders (id, customer_name) VALUES (1001, 'Tom');

UPDATE demo.orders
SET customer_name = 'Jerry'
WHERE id = 1001;

DELETE FROM demo.orders
WHERE id = 1001;
```

只有提交后的行变化才会出现在 Binlog 中：

```sql
COMMIT;
```

### 第四步：读取事件

调用 `get_mysql_watch_events`：

```json
{
  "watchId": "a1b2c3d4...",
  "afterSequence": 0,
  "limit": 100
}
```

第一次读取使用 `afterSequence: 0`。返回结果类似：

```json
{
  "watchId": "a1b2c3d4...",
  "state": "running",
  "events": [
    {
      "sequence": 1,
      "eventId": "a1b2c3d4...:1",
      "database": "demo",
      "table": "orders",
      "operation": "insert",
      "before": null,
      "after": {
        "id": 1001,
        "customer_name": "Tom"
      },
      "timestamp": "2026-08-17T10:00:30+00:00",
      "binlogFile": "mysql-bin.000001",
      "binlogPosition": 1234,
      "gtid": null
    }
  ],
  "nextSequence": 1,
  "hasMore": false
}
```

### 第五步：继续分页读取

将上一次响应中的 `nextSequence` 作为下一次请求的 `afterSequence`：

```json
{
  "watchId": "a1b2c3d4...",
  "afterSequence": 1,
  "limit": 100
}
```

当 `hasMore` 为 `true` 时继续读取；为 `false` 时表示当前已经没有更多已保存事件。监听仍可能处于 `running` 状态，需要稍后再次查询。

### 第六步：查询监听状态

调用 `get_mysql_watch_status`：

```json
{
  "watchId": "a1b2c3d4..."
}
```

常见状态如下：

| 状态 | 含义 |
|---|---|
| `starting` | 已创建会话，后台监听尚未完全开始 |
| `running` | 正在读取 Binlog |
| `completed` | 正常完成、达到时长或达到事件数量上限 |
| `stopped` | 用户主动停止或服务关闭 |
| `faulted` | 连接、权限或其他运行错误 |

### 第七步：主动停止监听

不需要继续监听时，调用 `stop_mysql_watch`：

```json
{
  "watchId": "a1b2c3d4..."
}
```

停止后，已经收集的事件仍然可以通过 `get_mysql_watch_events` 读取。

## 7. 工具参数速查

### `start_mysql_watch`

| 参数 | 必填 | 说明 |
|---|---:|---|
| `database` | 是 | 要监听的数据库名称 |
| `tables` | 否 | 表名数组；为空表示该数据库的所有表 |
| `operations` | 否 | `insert`、`update`、`delete`；为空表示全部操作 |
| `durationSeconds` | 否 | 监听时间，1 到 1800 秒，默认 300 秒（5 分钟） |
| `maxEvents` | 否 | 会话最多保留的事件数，1 到 100000，默认 1000 |

### `get_mysql_watch_events`

| 参数 | 必填 | 说明 |
|---|---:|---|
| `watchId` | 是 | `start_mysql_watch` 返回的 ID |
| `afterSequence` | 否 | 读取该序号之后的事件，默认 0 |
| `limit` | 否 | 本次最多返回 1000 条，默认 100 条 |

### `get_mysql_watch_targets`

不需要参数。返回当前活动监听的数据库、表过滤条件、操作过滤条件和有效期。`watches` 为空表示没有处于 `starting` 或 `running` 状态的监听。

### `get_mysql_watch_status` 和 `stop_mysql_watch`

两者都只需要一个参数：`watchId`。

## 8. 查询表结构和数据

### `get_mysql_table_schema`

读取指定表的列结构，不会修改数据库。返回每列的名称、数据类型、完整列类型、是否可空、键类型、默认值、额外属性和注释。

```json
{
  "database": "demo",
  "table": "orders"
}
```

### `get_mysql_table_data`

读取指定表的行数据，默认返回前 100 行，最多每次返回 1000 行。使用 `offset` 分页；响应中的 `nextOffset` 可直接用于下一次调用。工具只接受数据库名和表名，不接受原始 SQL 或 `WHERE` 子句。

```json
{
  "database": "demo",
  "table": "orders",
  "limit": 100,
  "offset": 0
}
```

返回结果包含 `columns`、`rows`、`nextOffset` 和 `hasMore`。查询账号需要目标数据库表的 `SELECT` 权限。

## 9. 事件字段说明

- `sequence`：会话内递增序号，用于分页。
- `eventId`：事件唯一标识，格式为 `watchId:sequence`。
- `database`、`table`：发生变化的数据库和表。
- `operation`：`insert`、`update` 或 `delete`。
- `before`：变化前的行数据。新增事件为空。
- `after`：变化后的行数据。删除事件为空。
- `timestamp`：Binlog 事件时间。
- `binlogFile`、`binlogPosition`：事件在 MySQL Binlog 中的位置。
- `gtid`：GTID 已启用时的事务标识，否则为空。

## 10. 常见问题

### MCP 客户端看不到工具

检查以下内容：

1. `command` 是否为 `DatabaseCdcMcp.exe` 的绝对路径。
2. JSON 中 Windows 路径的反斜杠是否写成 `\\`。
3. 修改配置后是否完全重启了桌面 MCP 客户端。
4. 发布目录中是否存在 `.exe` 文件。

### 启动时报 MySQL 未配置

确认 MCP 配置的 `env` 中包含：

```text
MYSQL_CDC_HOST
MYSQL_CDC_PORT
MYSQL_CDC_USER
MYSQL_CDC_PASSWORD
MYSQL_CDC_SERVER_ID
```

密码不会作为 MCP tool 参数传递。

### 监听成功但没有事件

按顺序检查：

1. `log_bin` 是否为 `ON`。
2. `binlog_format` 是否为 `ROW`。
3. `binlog_row_image` 是否为 `FULL`。
4. 账号是否拥有复制权限和目标数据库的 `SELECT` 权限。
5. SQL 是否在监听启动后执行并提交。
6. `database` 和 `tables` 是否拼写正确。
7. `operations` 是否包含实际执行的操作。

### 报权限错误

使用有权限的 MySQL 管理员重新执行账号授权 SQL，并确认授权的主机部分与实际连接来源匹配。授权修改后可以重新连接 MySQL，再重启 MCP 客户端。

### 报并发会话数量超限

当前同一时间只允许一个监听会话。先调用 `stop_mysql_watch` 停止旧会话，或等待旧会话达到时长后结束。

### 如何查看错误

这是 `stdio` MCP Server，协议数据使用标准输出，日志使用标准错误。请通过桌面 MCP 客户端的 MCP Server 日志查看连接失败、权限失败等错误，不要把程序当作普通控制台程序直接交互输入。

## 10. 开发命令

```powershell
dotnet restore src\DatabaseCdcMcp\DatabaseCdcMcp.csproj
dotnet restore tests\DatabaseCdcMcp.Tests\DatabaseCdcMcp.Tests.csproj
dotnet build src\DatabaseCdcMcp\DatabaseCdcMcp.csproj --no-restore
dotnet test tests\DatabaseCdcMcp.Tests\DatabaseCdcMcp.Tests.csproj --no-restore
```

重新发布 Windows self-contained 程序：

```powershell
dotnet publish src\DatabaseCdcMcp\DatabaseCdcMcp.csproj `
  -c Release `
  -r win-x64 `
  --self-contained true `
  -o artifacts\win-x64
```

发布文件位于 `artifacts\win-x64\`，其中的 `DatabaseCdcMcp.exe` 不需要目标机器预装 .NET 运行时。
