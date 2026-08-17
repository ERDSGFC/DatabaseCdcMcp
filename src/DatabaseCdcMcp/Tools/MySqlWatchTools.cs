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
    [Description("Starts a time-limited watch for new MySQL row changes and returns a watch identifier.")]
    public static StartWatchResponse StartMysqlWatch(
        WatchSessionManager manager,
        [Description("Database name to watch.")] string database,
        [Description("Optional table names. Empty means all tables in the database.")] string[]? tables = null,
        [Description("Optional operations: insert, update, delete. Empty means all operations.")] string[]? operations = null,
        [Description("Watch duration in seconds, from 1 to 1800.")] int durationSeconds = 60,
        [Description("Maximum retained events, from 1 to 100000.")] int maxEvents = 1000)
    {
        return Invoke(() => manager.Start(database, tables, operations, durationSeconds, maxEvents));
    }

    [McpServerTool(
        Name = "get_mysql_watch_events",
        Title = "Get MySQL watch events",
        ReadOnly = true,
        Destructive = false,
        Idempotent = true,
        OpenWorld = false,
        UseStructuredContent = true)]
    [Description("Returns events captured by a MySQL watch after the supplied sequence number.")]
    public static WatchEventsResponse GetMysqlWatchEvents(
        WatchSessionManager manager,
        [Description("Watch identifier returned by start_mysql_watch.")] string watchId,
        [Description("Return events with a sequence greater than this value.")] long afterSequence = 0,
        [Description("Maximum events to return, from 1 to 1000.")] int limit = 100)
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
    [Description("Returns the state and counters of a MySQL watch.")]
    public static WatchStatusResponse GetMysqlWatchStatus(
        WatchSessionManager manager,
        [Description("Watch identifier returned by start_mysql_watch.")] string watchId)
    {
        return Invoke(() => manager.GetStatus(watchId));
    }

    [McpServerTool(
        Name = "stop_mysql_watch",
        Title = "Stop MySQL watch",
        ReadOnly = false,
        Destructive = false,
        Idempotent = true,
        OpenWorld = false,
        UseStructuredContent = true)]
    [Description("Stops an active MySQL watch. Already finished watches remain readable.")]
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
