using DatabaseCdcMcp.Configuration;
using MySqlConnector;

namespace DatabaseCdcMcp.MySql;

/// <summary>
/// Executes read-only queries against the configured MySQL server.
/// </summary>
public sealed class MySqlQueryService(MySqlCdcSettings settings)
{
    private const int MaxRowsPerPage = 1_000;
    private const int MaxIdentifierLength = 64;
    private const int MaxLikePatternLength = 256;

    /// <summary>
    /// Returns a page of tables and views in a database, optionally filtered by a literal name prefix.
    /// </summary>
    public async Task<MySqlTablesResponse> GetTablesAsync(
        string database,
        string? tableNamePrefix = null,
        int limit = 100,
        long offset = 0,
        string? tableNameLike = null,
        CancellationToken cancellationToken = default)
    {
        var normalizedDatabase = NormalizeIdentifier(database, "database");
        var normalizedPrefix = NormalizeTableNamePrefix(tableNamePrefix);
        var normalizedLike = NormalizeTableNameLike(tableNameLike);
        if (normalizedPrefix is not null && normalizedLike is not null)
        {
            throw new MySqlQueryException("tableNamePrefix and tableNameLike cannot be used together.");
        }

        ValidatePagination(limit, offset);

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        var tableFilter = normalizedPrefix is not null
            ? "AND LEFT(TABLE_NAME, CHAR_LENGTH(@tableNamePrefix)) = @tableNamePrefix"
            : normalizedLike is not null
                ? "AND TABLE_NAME LIKE @tableNameLike"
                : string.Empty;

        command.CommandText = $"""
            SELECT
                TABLE_NAME,
                TABLE_TYPE,
                ENGINE,
                TABLE_COMMENT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @database
              {tableFilter}
            ORDER BY TABLE_NAME
            LIMIT @limit OFFSET @offset
            """;
        command.Parameters.AddWithValue("@database", normalizedDatabase);
        if (normalizedPrefix is not null)
        {
            command.Parameters.AddWithValue("@tableNamePrefix", normalizedPrefix);
        }
        else if (normalizedLike is not null)
        {
            command.Parameters.AddWithValue("@tableNameLike", normalizedLike);
        }

        command.Parameters.Add("@limit", MySqlDbType.Int32).Value = limit + 1;
        command.Parameters.Add("@offset", MySqlDbType.Int64).Value = offset;

        var tables = new List<MySqlTableSummary>(limit);
        var hasMore = false;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (tables.Count == limit)
            {
                hasMore = true;
                break;
            }

            tables.Add(new MySqlTableSummary(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetString(3)));
        }

        return new MySqlTablesResponse(
            normalizedDatabase,
            normalizedPrefix,
            normalizedLike,
            tables,
            offset,
            limit,
            checked(offset + tables.Count),
            hasMore);
    }

    /// <summary>
    /// Returns the column metadata for a table in ordinal order.
    /// </summary>
    public async Task<MySqlTableSchemaResponse> GetTableSchemaAsync(
        string database,
        string table,
        CancellationToken cancellationToken = default)
    {
        var normalizedDatabase = NormalizeIdentifier(database, "database");
        var normalizedTable = NormalizeIdentifier(table, "table");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                ORDINAL_POSITION,
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_KEY,
                COLUMN_DEFAULT,
                EXTRA,
                COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @database AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """;
        command.Parameters.AddWithValue("@database", normalizedDatabase);
        command.Parameters.AddWithValue("@table", normalizedTable);

        var columns = new List<MySqlColumnSchema>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(new MySqlColumnSchema(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetValue(6),
                reader.GetString(7),
                reader.GetString(8)));
        }

        if (columns.Count == 0)
        {
            throw new MySqlQueryException(
                $"Table '{normalizedDatabase}.{normalizedTable}' does not exist or is not visible to the configured MySQL user.");
        }

        return new MySqlTableSchemaResponse(normalizedDatabase, normalizedTable, columns);
    }

    /// <summary>
    /// Returns a page of rows from a table. The extra row is used only to calculate HasMore.
    /// </summary>
    public async Task<MySqlTableDataResponse> GetTableDataAsync(
        string database,
        string table,
        int limit = 100,
        long offset = 0,
        CancellationToken cancellationToken = default)
    {
        var normalizedDatabase = NormalizeIdentifier(database, "database");
        var normalizedTable = NormalizeIdentifier(table, "table");
        ValidatePagination(limit, offset);

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {QuoteIdentifier(normalizedDatabase)}.{QuoteIdentifier(normalizedTable)} LIMIT @limit OFFSET @offset";
        command.Parameters.Add("@limit", MySqlDbType.Int32).Value = limit + 1;
        command.Parameters.Add("@offset", MySqlDbType.Int64).Value = offset;

        var columns = new List<string>();
        var rows = new List<IReadOnlyDictionary<string, object?>>(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        for (var columnIndex = 0; columnIndex < reader.FieldCount; columnIndex++)
        {
            columns.Add(reader.GetName(columnIndex));
        }

        var hasMore = false;
        while (await reader.ReadAsync(cancellationToken))
        {
            if (rows.Count == limit)
            {
                hasMore = true;
                break;
            }

            var row = new Dictionary<string, object?>(reader.FieldCount, StringComparer.OrdinalIgnoreCase);
            for (var columnIndex = 0; columnIndex < reader.FieldCount; columnIndex++)
            {
                row[columns[columnIndex]] = reader.IsDBNull(columnIndex) ? null : reader.GetValue(columnIndex);
            }

            rows.Add(row);
        }

        return new MySqlTableDataResponse(
            normalizedDatabase,
            normalizedTable,
            columns,
            rows,
            offset,
            limit,
            checked(offset + rows.Count),
            hasMore);
    }

    private async Task<MySqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        if (!settings.IsConfigured)
        {
            throw new MySqlQueryException(
                "MySQL is not configured. Set MYSQL_CDC_HOST, MYSQL_CDC_USER and MYSQL_CDC_PASSWORD before querying the MCP server.");
        }

        var connectionString = new MySqlConnectionStringBuilder
        {
            Server = settings.Hostname,
            Port = checked((uint)settings.Port),
            UserID = settings.Username,
            Password = settings.Password,
            SslMode = MySqlSslMode.None,
            ConnectionTimeout = 10,
            DefaultCommandTimeout = 30
        }.ConnectionString;

        try
        {
            var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch (Exception exception) when (exception is MySqlException or InvalidOperationException)
        {
            throw new MySqlQueryException("Unable to connect to MySQL.", exception);
        }
    }

    private static string NormalizeIdentifier(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new MySqlQueryException($"{parameterName} is required.");
        }

        var normalized = value.Trim();
        if (normalized.Length > MaxIdentifierLength || normalized.Contains('\0'))
        {
            throw new MySqlQueryException($"{parameterName} must be a valid MySQL identifier no longer than {MaxIdentifierLength} characters.");
        }

        return normalized;
    }

    private static string? NormalizeTableNamePrefix(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var normalized = value.Trim();
        if (normalized.Length > MaxIdentifierLength || normalized.Contains('\0'))
        {
            throw new MySqlQueryException(
                $"tableNamePrefix must be no longer than {MaxIdentifierLength} characters and cannot contain a null character.");
        }

        return normalized;
    }

    private static string? NormalizeTableNameLike(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var normalized = value.Trim();
        if (normalized.Length > MaxLikePatternLength || normalized.Contains('\0'))
        {
            throw new MySqlQueryException(
                $"tableNameLike must be no longer than {MaxLikePatternLength} characters and cannot contain a null character.");
        }

        return normalized;
    }

    private static void ValidatePagination(int limit, long offset)
    {
        if (limit is < 1 or > MaxRowsPerPage)
        {
            throw new MySqlQueryException($"limit must be between 1 and {MaxRowsPerPage}.");
        }

        if (offset < 0)
        {
            throw new MySqlQueryException("offset must be zero or greater.");
        }
    }

    private static string QuoteIdentifier(string identifier) => $"`{identifier.Replace("`", "``", StringComparison.Ordinal)}`";
}

public sealed record MySqlColumnSchema(
    int OrdinalPosition,
    string Name,
    string DataType,
    string ColumnType,
    string IsNullable,
    string Key,
    object? DefaultValue,
    string Extra,
    string Comment);

public sealed record MySqlTableSchemaResponse(
    string Database,
    string Table,
    IReadOnlyList<MySqlColumnSchema> Columns);

public sealed record MySqlTableSummary(
    string Name,
    string TableType,
    string? Engine,
    string Comment);

public sealed record MySqlTablesResponse(
    string Database,
    string? TableNamePrefix,
    string? TableNameLike,
    IReadOnlyList<MySqlTableSummary> Tables,
    long Offset,
    int Limit,
    long NextOffset,
    bool HasMore);

public sealed record MySqlTableDataResponse(
    string Database,
    string Table,
    IReadOnlyList<string> Columns,
    IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows,
    long Offset,
    int Limit,
    long NextOffset,
    bool HasMore);

public sealed class MySqlQueryException(string message, Exception? innerException = null)
    : Exception(message, innerException);
