using System.Runtime.CompilerServices;
using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.Watches;
using MySqlCdc;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlConnector;

namespace DatabaseCdcMcp.MySql;

public sealed class MySqlCdcChangeStreamFactory(MySqlCdcSettings settings)
    : IMySqlChangeStreamFactory
{
    public async IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
        MySqlWatchRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var client = CreateClient(request.Database);
        var tableMaps = new Dictionary<long, TableContext>();
        var columnNameCache = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

        await foreach (var (header, binlogEvent) in client.Replicate(cancellationToken))
        {
            if (binlogEvent is TableMapEvent tableMap)
            {
                if (!MatchesTarget(request, tableMap.DatabaseName, tableMap.TableName))
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

            switch (binlogEvent)
            {
                case WriteRowsEvent writeRows
                    when request.Operations.Contains(ChangeOperation.Insert) &&
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

                case UpdateRowsEvent updateRows
                    when request.Operations.Contains(ChangeOperation.Update) &&
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

                case DeleteRowsEvent deleteRows
                    when request.Operations.Contains(ChangeOperation.Delete) &&
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

    private BinlogClient CreateClient(string database)
    {
        return new BinlogClient(options =>
        {
            options.Hostname = settings.Hostname;
            options.Port = settings.Port;
            options.Username = settings.Username;
            options.Password = settings.Password;
            options.Database = database;
            options.ServerId = settings.ServerId;
            options.SslMode = SslMode.Disabled;
            options.Blocking = true;
            options.HeartbeatInterval = TimeSpan.FromSeconds(15);
            options.Binlog = BinlogOptions.FromEnd();
        });
    }

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

    private static bool MatchesTarget(MySqlWatchRequest request, string database, string table)
    {
        return string.Equals(request.Database, database, StringComparison.OrdinalIgnoreCase) &&
               (request.Tables.Count == 0 || request.Tables.Contains(table));
    }

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
