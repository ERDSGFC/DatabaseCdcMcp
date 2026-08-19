using System.Runtime.CompilerServices;
using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.Watches;
using MySqlCdc;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlConnector;

namespace DatabaseCdcMcp.MySql;

/// <summary>
/// 创建 MySQL Binlog 数据流，并将行事件转换为应用程序的数据库变化模型。
/// </summary>
/// <param name="settings">源 MySQL 服务器的连接和复制配置。</param>
public sealed class MySqlCdcChangeStreamFactory(MySqlCdcSettings settings)
    : IMySqlChangeStreamFactory
{
    /// <summary>
    /// 从当前 MySQL Binlog 末尾读取已提交的行变化，供所有逻辑监听共享。
    /// </summary>
    /// <param name="shouldCaptureTable">判断当前是否至少有一个逻辑监听需要指定表。</param>
    /// <param name="cancellationToken">监听结束时用于停止复制数据流。</param>
    /// <returns>标准化后的新增、更新和删除事件流。</returns>
    public async IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
        Func<string, string, bool> shouldCaptureTable,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shouldCaptureTable);

        var client = CreateClient();

        // Binlog 行事件通过内部数字 ID 引用表。
        // 保存最新的表映射，后续行事件才能按表名解析。
        var tableMaps = new Dictionary<long, TableContext>();

        // Binlog 中会重复携带表元数据。
        // 对同一个数据库和表只查询一次 INFORMATION_SCHEMA。
        var columnNameCache = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

        await foreach (var (header, binlogEvent) in client.Replicate(cancellationToken))
        {
            if (binlogEvent is TableMapEvent tableMap)
            {
                // 必须先读取表映射，才能解析该表后续的行事件。
                if (!shouldCaptureTable(tableMap.DatabaseName, tableMap.TableName))
                {
                    tableMaps.Remove(tableMap.TableId);
                    continue;
                }

                var cacheKey = $"{tableMap.DatabaseName}.{tableMap.TableName}";
                if (!columnNameCache.TryGetValue(cacheKey, out var columnNames))
                {
                    columnNames = await ResolveColumnNamesAsync(tableMap, cancellationToken);
                    columnNameCache[cacheKey] = columnNames;
                }

                tableMaps[tableMap.TableId] = new TableContext(
                    tableMap.DatabaseName,
                    tableMap.TableName,
                    columnNames);

                continue;
            }

            // 一个行事件可能包含多行数据，因此每行生成一个标准化事件，
            // 同时保留该事件对应的 Binlog 位点。
            switch (binlogEvent)
            {
                case WriteRowsEvent writeRows when
                    tableMaps.TryGetValue(writeRows.TableId, out var writeContext):
                    foreach (var row in writeRows.Rows)
                    {
                        yield return CreateChange(
                            client,
                            header,
                            writeContext,
                            ChangeOperation.Insert,
                            null,
                            MapRow(writeContext.ColumnNames, row.Cells));
                    }

                    break;

                case UpdateRowsEvent updateRows when
                    tableMaps.TryGetValue(updateRows.TableId, out var updateContext):
                    foreach (var row in updateRows.Rows)
                    {
                        yield return CreateChange(
                            client,
                            header,
                            updateContext,
                            ChangeOperation.Update,
                            MapRow(updateContext.ColumnNames, row.BeforeUpdate.Cells),
                            MapRow(updateContext.ColumnNames, row.AfterUpdate.Cells));
                    }

                    break;

                case DeleteRowsEvent deleteRows when
                    tableMaps.TryGetValue(deleteRows.TableId, out var deleteContext):
                    foreach (var row in deleteRows.Rows)
                    {
                        yield return CreateChange(
                            client,
                            header,
                            deleteContext,
                            ChangeOperation.Delete,
                            MapRow(deleteContext.ColumnNames, row.Cells),
                            null);
                    }

                    break;
            }
        }
    }

    private BinlogClient CreateClient()
    {
        return new BinlogClient(options =>
        {
            options.Hostname = settings.Hostname;
            options.Port = settings.Port;
            options.Username = settings.Username;
            options.Password = settings.Password;
            options.ServerId = settings.ServerId;
            options.SslMode = SslMode.Disabled;
            options.Blocking = true;
            options.HeartbeatInterval = TimeSpan.FromSeconds(15);

            // 短时监听只需要返回监听启动之后产生的变化，因此从 Binlog 末尾开始。
            options.Binlog = BinlogOptions.FromEnd();
        });
    }

    /// <summary>
    /// 从 Binlog 元数据或 INFORMATION_SCHEMA 中解析列名。
    /// </summary>
    /// <remarks>
    /// 如果两者都无法提供完整的表结构，则返回按位置生成的备用列名，
    /// 保证数据仍然可以被观察到，而不是直接丢弃整行。
    /// </remarks>
    private async Task<IReadOnlyList<string>> ResolveColumnNamesAsync(
        TableMapEvent tableMap,
        CancellationToken cancellationToken)
    {
        var metadataNames = tableMap.TableMetadata?.ColumnNames;
        if (metadataNames is { Count: > 0 } && metadataNames.Count == tableMap.ColumnTypes.Length)
        {
            return metadataNames;
        }

        var connectionString = new MySqlConnectionStringBuilder
        {
            Server = settings.Hostname,
            Port = checked((uint)settings.Port),
            UserID = settings.Username,
            Password = settings.Password,
            Database = tableMap.DatabaseName,
            SslMode = MySqlSslMode.None,
            ConnectionTimeout = 10,
            DefaultCommandTimeout = 10
        }.ConnectionString;

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @database AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """;
        command.Parameters.AddWithValue("@database", tableMap.DatabaseName);
        command.Parameters.AddWithValue("@table", tableMap.TableName);

        var names = new List<string>(tableMap.ColumnTypes.Length);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        if (names.Count == tableMap.ColumnTypes.Length)
        {
            return names;
        }

        return Enumerable.Range(1, tableMap.ColumnTypes.Length)
            .Select(index => $"column_{index}")
            .ToArray();
    }

    /// <summary>
    /// 将行单元格映射为列名，同时保留空值和驱动程序返回的原始值类型。
    /// </summary>
    private static IReadOnlyDictionary<string, object?> MapRow(
        IReadOnlyList<string> columnNames,
        IReadOnlyList<object?> cells)
    {
        var values = new Dictionary<string, object?>(cells.Count, StringComparer.OrdinalIgnoreCase);

        for (var index = 0; index < cells.Count; index++)
        {
            var name = index < columnNames.Count ? columnNames[index] : $"column_{index + 1}";
            values[name] = cells[index];
        }

        return values;
    }

    /// <summary>
    /// 创建所有行操作共用的事件封装对象。
    /// </summary>
    private static DatabaseChange CreateChange(
        BinlogClient client,
        EventHeader header,
        TableContext context,
        ChangeOperation operation,
        IReadOnlyDictionary<string, object?>? before,
        IReadOnlyDictionary<string, object?>? after)
    {
        var timestamp = header.Timestamp > 0
            ? DateTimeOffset.FromUnixTimeSeconds(header.Timestamp)
            : DateTimeOffset.UtcNow;

        return new DatabaseChange(
            0,
            string.Empty,
            context.Database,
            context.Table,
            operation,
            before,
            after,
            timestamp,
            client.State.Filename,
            header.NextEventPosition,
            client.State.GtidState?.ToString());
    }

    private sealed record TableContext(
        string Database,
        string Table,
        IReadOnlyList<string> ColumnNames);
}
