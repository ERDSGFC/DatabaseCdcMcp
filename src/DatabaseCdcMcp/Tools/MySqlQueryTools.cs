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
    [Description("Returns the columns and metadata of a MySQL table.")]
    public static Task<MySqlTableSchemaResponse> GetMysqlTableSchema(
        MySqlQueryService queryService,
        [Description("Database name containing the table.")] string database,
        [Description("Table name whose schema should be returned.")] string table,
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
    [Description("Returns a paginated set of rows from a MySQL table.")]
    public static Task<MySqlTableDataResponse> GetMysqlTableData(
        MySqlQueryService queryService,
        [Description("Database name containing the table.")] string database,
        [Description("Table name whose rows should be returned.")] string table,
        [Description("Maximum rows to return, from 1 to 1000.")] int limit = 100,
        [Description("Number of rows to skip before returning data.")] long offset = 0,
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
