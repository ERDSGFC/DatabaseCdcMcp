using System.ComponentModel;
using DatabaseCdcMcp.Watches;
using ModelContextProtocol;
using ModelContextProtocol.Server;

namespace DatabaseCdcMcp.Tools;

[McpServerToolType]
public static class MySqlWatchTools
{
    [McpServerTool(
        Name = "start_mysql_watch",
        Title = "Start MySQL watch",
        ReadOnly = false,
        Destructive = false,
        OpenWorld = true,
        UseStructuredContent = true)]
    [Description(
        "Use this when the user wants to monitor future committed MySQL row changes. " +
        "Start the watch before the INSERT, UPDATE, or DELETE occurs. It reads only new " +
        "row-level binlog events, not historical data. Up to 32 logical watches can run " +
        "concurrently and share one MySQL binlog connection.")]
    public static StartWatchResponse StartMysqlWatch(
        WatchSessionManager manager,
        [Description("Exact MySQL database name to monitor. The database must already exist and be accessible to the configured user.")] string database,
        [Description("Optional exact table names to monitor. An empty or omitted array monitors every table in the database.")] string[]? tables = null,
        [Description("Optional operation filter. Allowed values are insert, update, and delete. An empty or omitted array includes all three operations.")] string[]? operations = null,
        [Description("How long to collect events, in seconds. Must be between 1 and 3600; defaults to 600 seconds. The watch expires automatically.")] int durationSeconds = 600,
        [Description("Maximum number of complete committed transactions to retain before the watch finishes. Must be between 1 and 100000; defaults to 1000.")] int maxTransactions = 1000)
    {
        return Invoke(() => manager.Start(database, tables, operations, durationSeconds, maxTransactions));
    }

    [McpServerTool(
        Name = "get_mysql_watch_events",
        Title = "Get MySQL watch events",
        ReadOnly = true,
        Destructive = false,
        Idempotent = true,
        OpenWorld = false,
        UseStructuredContent = true)]
    [Description(
        "Returns committed transactions already captured by a watch, with all matching row changes " +
        "from each transaction kept together. Call this after start_mysql_watch using its watchId, " +
        "and pass the previous nextSequence as afterSequence for transaction pagination. Data is " +
        "held in memory only and is removed when the process exits.")]
    public static WatchEventsResponse GetMysqlWatchEvents(
        WatchSessionManager manager,
        [Description("Watch identifier returned by start_mysql_watch.")] string watchId,
        [Description("Return transactions whose sequence is greater than this value. Use 0 for the first page, then use the previous response's nextSequence.")] long afterSequence = 0,
        [Description("Maximum complete transactions in this page. Must be between 1 and 1000; defaults to 100.")] int limit = 100)
    {
        return Invoke(() => manager.GetEvents(watchId, afterSequence, limit));
    }

    [McpServerTool(
        Name = "get_mysql_watch_status",
        Title = "Get MySQL watch status",
        ReadOnly = true,
        Destructive = false,
        Idempotent = true,
        OpenWorld = false,
        UseStructuredContent = true)]
    [Description("Returns the current state, expiration time, retained transaction and row-change counts, and any error for a watch. Use this to check whether collection is still running or has completed.")]
    public static WatchStatusResponse GetMysqlWatchStatus(
        WatchSessionManager manager,
        [Description("Watch identifier returned by start_mysql_watch.")] string watchId)
    {
        return Invoke(() => manager.GetStatus(watchId));
    }

    [McpServerTool(
        Name = "get_mysql_watch_targets",
        Title = "Get current MySQL watch targets",
        ReadOnly = true,
        Destructive = false,
        Idempotent = true,
        OpenWorld = false,
        UseStructuredContent = true)]
    [Description("Lists the database, table filter, operation filter, and expiration time for all currently active watches. Use this to discover active targets without a watchId.")]
    public static WatchTargetsResponse GetMysqlWatchTargets(WatchSessionManager manager)
    {
        return manager.GetCurrentTargets();
    }

    [McpServerTool(
        Name = "stop_mysql_watch",
        Title = "Stop MySQL watch",
        ReadOnly = false,
        Destructive = false,
        Idempotent = true,
        OpenWorld = false,
        UseStructuredContent = true)]
    [Description("Stops an active watch early and returns its final status. Events already captured remain readable with get_mysql_watch_events after stopping.")]
    public static WatchStatusResponse StopMysqlWatch(
        WatchSessionManager manager,
        [Description("Watch identifier returned by start_mysql_watch.")] string watchId)
    {
        return Invoke(() => manager.Stop(watchId));
    }

    private static T Invoke<T>(Func<T> action)
    {
        try
        {
            return action();
        }
        catch (WatchException exception)
        {
            throw new McpException(exception.Message, exception);
        }
    }
}
