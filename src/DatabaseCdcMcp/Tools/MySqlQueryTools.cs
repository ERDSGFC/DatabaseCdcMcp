using System.ComponentModel;
using DatabaseCdcMcp.MySql;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using MySqlConnector;

namespace DatabaseCdcMcp.Tools;

[McpServerToolType]
public static class MySqlQueryTools
{
    [McpServerTool(
        Name = "get_mysql_table_schema",
        Title = "Get MySQL table schema",
        ReadOnly = true,
        Destructive = false,
        Idempotent = true,
        OpenWorld = true,
        UseStructuredContent = true)]
    [Description("Returns the column definitions and metadata for an existing MySQL table. Use this before interpreting row values when column names or types are unknown. This tool does not modify data.")]
    public static Task<MySqlTableSchemaResponse> GetMysqlTableSchema(
        MySqlQueryService queryService,
        [Description("Exact MySQL database name containing the table.")] string database,
        [Description("Exact MySQL table name whose column definitions should be returned.")] string table,
        CancellationToken cancellationToken = default)
    {
        return InvokeAsync(() => queryService.GetTableSchemaAsync(database, table, cancellationToken));
    }

    [McpServerTool(
        Name = "get_mysql_table_data",
        Title = "Get MySQL table data",
        ReadOnly = true,
        Destructive = false,
        Idempotent = true,
        OpenWorld = true,
        UseStructuredContent = true)]
    [Description("Reads a paginated set of rows from an existing MySQL table. Use this for current table data or an initial snapshot; it does not read binlog history and does not modify data.")]
    public static Task<MySqlTableDataResponse> GetMysqlTableData(
        MySqlQueryService queryService,
        [Description("Exact MySQL database name containing the table.")] string database,
        [Description("Exact MySQL table name whose current rows should be returned.")] string table,
        [Description("Maximum rows in this page. Must be between 1 and 1000; defaults to 100.")] int limit = 100,
        [Description("Number of rows to skip before this page. Use the previous response's nextOffset for pagination; defaults to 0.")] long offset = 0,
        CancellationToken cancellationToken = default)
    {
        return InvokeAsync(() => queryService.GetTableDataAsync(database, table, limit, offset, cancellationToken));
    }

    private static async Task<T> InvokeAsync<T>(Func<Task<T>> action)
    {
        try
        {
            return await action();
        }
        catch (MySqlQueryException exception)
        {
            throw new McpException(exception.Message, exception);
        }
        catch (MySqlException exception)
        {
            throw new McpException("MySQL query failed. Check the database, table and configured user's permissions.", exception);
        }
    }
}
