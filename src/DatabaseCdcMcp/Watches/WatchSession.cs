using DatabaseCdcMcp.Domain;

namespace DatabaseCdcMcp.Watches;

internal sealed class WatchSession
{
    private readonly object _gate = new();
    private readonly List<DatabaseChange> _events = [];
    private readonly TaskCompletionSource _completionSource = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private WatchState _state = WatchState.Starting;
    private DateTimeOffset? _finishedAt;
    private string? _finishReason;
    private string? _error;

    public WatchSession(string id, MySqlWatchRequest request, DateTimeOffset startedAt)
    {
        Id = id;
        Request = request;
        StartedAt = startedAt;
        ExpiresAt = startedAt.Add(request.Duration);
    }

    public string Id { get; }

    public MySqlWatchRequest Request { get; }

    public DateTimeOffset StartedAt { get; }

    public DateTimeOffset ExpiresAt { get; }

    public Task Completion => _completionSource.Task;

    public bool IsActive
    {
        get
        {
            lock (_gate)
            {
                return IsActiveState(_state);
            }
        }
    }

    public bool MatchesTarget(string database, string table)
    {
        lock (_gate)
        {
            return IsActiveState(_state) &&
                   string.Equals(Request.Database, database, StringComparison.OrdinalIgnoreCase) &&
                   (Request.Tables.Count == 0 || Request.Tables.Contains(table));
        }
    }

    public bool TryAddEvent(DatabaseChange change, out bool reachedLimit)
    {
        lock (_gate)
        {
            reachedLimit = false;

            if (!IsActiveState(_state) ||
                !string.Equals(Request.Database, change.Database, StringComparison.OrdinalIgnoreCase) ||
                (Request.Tables.Count > 0 && !Request.Tables.Contains(change.Table)) ||
                !Request.Operations.Contains(change.Operation) ||
                _events.Count >= Request.MaxEvents)
            {
                return false;
            }

            var sequence = _events.Count + 1L;
            _events.Add(change with
            {
                Sequence = sequence,
                EventId = $"{Id}:{sequence}"
            });

            reachedLimit = _events.Count >= Request.MaxEvents;
            return true;
        }
    }

    public void MarkRunning()
    {
        lock (_gate)
        {
            if (_state == WatchState.Starting)
            {
                _state = WatchState.Running;
            }
        }
    }

    public void Complete(string reason) => Finish(WatchState.Completed, reason, null);

    public void Fail(Exception exception) =>
        Finish(WatchState.Faulted, "listener_error", exception.Message);

    public void MarkStopped(string reason) => Finish(WatchState.Stopped, reason, null);

    public bool RequestStop() => Finish(WatchState.Stopped, "stopped_by_user", null);

    public WatchStatusResponse GetStatus()
    {
        lock (_gate)
        {
            return new WatchStatusResponse(
                Id,
                FormatState(_state),
                _events.Count,
                StartedAt,
                ExpiresAt,
                _finishedAt,
                _finishReason,
                _error);
        }
    }

    public WatchTargetResponse? GetTarget()
    {
        lock (_gate)
        {
            if (!IsActiveState(_state))
            {
                return null;
            }

            return new WatchTargetResponse(
                Id,
                FormatState(_state),
                Request.Database,
                Request.Tables.Count == 0,
                Request.Tables.OrderBy(table => table, StringComparer.OrdinalIgnoreCase).ToArray(),
                Request.Operations
                    .OrderBy(operation => operation)
                    .Select(operation => operation.ToString().ToLowerInvariant())
                    .ToArray(),
                StartedAt,
                ExpiresAt);
        }
    }

    public WatchEventsResponse GetEvents(long afterSequence, int limit)
    {
        lock (_gate)
        {
            var events = _events
                .Where(change => change.Sequence > afterSequence)
                .Take(limit)
                .ToArray();

            var nextSequence = events.Length == 0 ? afterSequence : events[^1].Sequence;
            var hasMore = _events.Count > nextSequence;

            return new WatchEventsResponse(
                Id,
                FormatState(_state),
                events,
                nextSequence,
                hasMore);
        }
    }

    private bool Finish(WatchState state, string reason, string? error)
    {
        lock (_gate)
        {
            if (!IsActiveState(_state))
            {
                return false;
            }

            _state = state;
            _finishReason = reason;
            _error = error;
            _finishedAt = DateTimeOffset.UtcNow;
        }

        _completionSource.TrySetResult();
        return true;
    }

    private static bool IsActiveState(WatchState state) =>
        state is WatchState.Starting or WatchState.Running;

    private static string FormatState(WatchState state) => state.ToString().ToLowerInvariant();
}
