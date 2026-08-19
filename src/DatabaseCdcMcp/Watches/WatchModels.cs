using System.ComponentModel;
using DatabaseCdcMcp.Domain;

namespace DatabaseCdcMcp.Watches;

public sealed record MySqlWatchRequest(
    string Database,
    IReadOnlySet<string> Tables,
    IReadOnlySet<ChangeOperation> Operations,
    TimeSpan Duration,
    int MaxEvents);

[Description("Information returned immediately after a MySQL watch is created.")]
public sealed record StartWatchResponse(
    [property: Description("Identifier to use with the watch status, events, and stop tools.")]
    string WatchId,
    [property: Description("Current watch state: starting, running, completed, stopped, or faulted.")]
    string State,
    [property: Description("UTC timestamp when the watch was created.")]
    DateTimeOffset StartedAt,
    [property: Description("UTC timestamp when the watch is scheduled to expire.")]
    DateTimeOffset ExpiresAt,
    [property: Description("Maximum number of events retained in memory for this watch.")]
    int MaxEvents);

[Description("Current lifecycle state and event count for a MySQL watch.")]
public sealed record WatchStatusResponse(
    [property: Description("Watch identifier returned by start_mysql_watch.")]
    string WatchId,
    [property: Description("Current watch state: starting, running, completed, stopped, or faulted.")]
    string State,
    [property: Description("Number of row-change events currently retained for this watch.")]
    int EventCount,
    [property: Description("UTC timestamp when the watch was created.")]
    DateTimeOffset StartedAt,
    [property: Description("UTC timestamp when the watch is scheduled to expire.")]
    DateTimeOffset ExpiresAt,
    [property: Description("UTC timestamp when the watch finished; null while it is starting or running.")]
    DateTimeOffset? FinishedAt,
    [property: Description("Machine-readable completion reason, such as duration_elapsed, max_events_reached, stopped_by_user, stream_ended, server_shutdown, or listener_error; null while active.")]
    string? FinishReason,
    [property: Description("Listener error message when state is faulted; otherwise null.")]
    string? Error);

[Description("Database, table, and operation filters for an active MySQL watch.")]
public sealed record WatchTargetResponse(
    [property: Description("Watch identifier returned by start_mysql_watch.")]
    string WatchId,
    [property: Description("Current watch state: starting or running.")]
    string State,
    [property: Description("Exact MySQL database being monitored.")]
    string Database,
    [property: Description("True when every table in the database is monitored.")]
    bool AllTables,
    [property: Description("Exact monitored table names; empty when allTables is true.")]
    IReadOnlyList<string> Tables,
    [property: Description("Monitored operation names: insert, update, and/or delete.")]
    IReadOnlyList<string> Operations,
    [property: Description("UTC timestamp when the watch was created.")]
    DateTimeOffset StartedAt,
    [property: Description("UTC timestamp when the watch is scheduled to expire.")]
    DateTimeOffset ExpiresAt);

[Description("The currently active MySQL watch targets; completed watches are omitted.")]
public sealed record WatchTargetsResponse(
    [property: Description("Active watches. This collection is empty when no watch is starting or running.")]
    IReadOnlyList<WatchTargetResponse> Watches);

[Description("A page of row-change events captured by one MySQL watch.")]
public sealed record WatchEventsResponse(
    [property: Description("Watch identifier returned by start_mysql_watch.")]
    string WatchId,
    [property: Description("Current watch state: starting, running, completed, stopped, or faulted.")]
    string State,
    [property: Description("Captured events whose sequence is greater than the requested afterSequence, ordered by sequence.")]
    IReadOnlyList<DatabaseChange> Events,
    [property: Description("Sequence to pass as afterSequence on the next request. It equals the last returned event sequence, or the requested afterSequence when no events were returned.")]
    long NextSequence,
    [property: Description("True when additional captured events are already available after nextSequence.")]
    bool HasMore);

internal enum WatchState
{
    Starting,
    Running,
    Completed,
    Stopped,
    Faulted
}
