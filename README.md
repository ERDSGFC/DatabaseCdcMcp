# Database CDC MCP

一个面向桌面 MCP 客户端的轻量 MySQL 数据变化监听服务。服务从当前 Binlog 末尾开始，在限定时间内收集目标表的新增、更新和删除事件。

> 当前处于 MVP 实现阶段，进度见 [PLAN.md](PLAN.md)。

## 配置

数据库凭据不通过 MCP tool 传递。启动服务前设置以下环境变量：

```powershell
$env:MYSQL_CDC_HOST = "127.0.0.1"
$env:MYSQL_CDC_PORT = "3306"
$env:MYSQL_CDC_USER = "cdc_user"
$env:MYSQL_CDC_PASSWORD = "change-me"
$env:MYSQL_CDC_SERVER_ID = "6174"
```

MySQL 必须开启 ROW Binlog：

```ini
binlog_format=ROW
binlog_row_image=FULL
```

监听账号需要复制权限；服务还会读取 `INFORMATION_SCHEMA.COLUMNS` 将 Binlog 单元格映射为列名，因此也需要目标库的 `SELECT` 权限：

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON your_database.* TO 'cdc_user'@'%';
```

MySqlCdc 4.0.1 尚未完全支持 SSL。当前版本显式关闭 Binlog 连接 SSL，只应连接可信本地网络中的 MySQL。

## 开发命令

```powershell
dotnet restore
dotnet build --no-restore
dotnet test --no-build
```

## 发布

```powershell
dotnet publish src/DatabaseCdcMcp/DatabaseCdcMcp.csproj -c Release -r win-x64 --self-contained true
```

## MCP 配置示例

```json
{
  "mcpServers": {
    "database-cdc": {
      "command": "C:\\path\\to\\DatabaseCdcMcp.exe",
      "env": {
        "MYSQL_CDC_HOST": "127.0.0.1",
        "MYSQL_CDC_PORT": "3306",
        "MYSQL_CDC_USER": "cdc_user",
        "MYSQL_CDC_PASSWORD": "change-me",
        "MYSQL_CDC_SERVER_ID": "6174"
      }
    }
  }
}
```
