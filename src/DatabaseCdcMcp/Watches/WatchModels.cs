using DatabaseCdcMcp.Domain;

namespace DatabaseCdcMcp.Watches;

public sealed record MySqlWatchRequest(
    string Database,
    IReadOnlySet<string> Tables,
    IReadOnlySet<ChangeOperation> Operations,
    TimeSpan Duration,
    int MaxEvents);

public sealed record StartWatchResponse(
    string WatchId,
    string State,
    DateTimeOffset StartedAt,
    DateTimeOffset ExpiresAt,
    int MaxEvents);

public sealed record WatchStatusResponse(
    string WatchId,
    string State,
    int EventCount,
    DateTimeOffset StartedAt,
    DateTimeOffset ExpiresAt,
    DateTimeOffset? FinishedAt,
    string? FinishReason,
    string? Error);

public sealed record WatchEventsResponse(
    string WatchId,
    string State,
    IReadOnlyList<DatabaseChange> Events,
    long NextSequence,
    bool HasMore);

internal enum WatchState
{
    Starting,
    Running,
    Completed,
    Stopped,
    Faulted
}
