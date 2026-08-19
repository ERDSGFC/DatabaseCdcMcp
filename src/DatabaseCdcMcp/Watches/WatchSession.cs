using DatabaseCdcMcp.Domain;

namespace DatabaseCdcMcp.Watches;

internal sealed class WatchSession(
    string id,
    MySqlWatchRequest request,
    DateTimeOffset startedAt,
    int maxRetainedChanges,
    int maxChangesPerTransaction)
{
    private readonly Lock _gate = new();
    private readonly List<DatabaseTransaction> _transactions = [];
    private readonly TaskCompletionSource _completionSource = new(
        TaskCreationOptions.RunContinuationsAsynchronously);

    private int _changeCount;
    private WatchState _state = WatchState.Starting;
    private DateTimeOffset? _finishedAt;
    private string? _finishReason;
    private string? _error;

    public string Id { get; } = id;

    public MySqlWatchRequest Request { get; } = request;

    public DateTimeOffset StartedAt { get; } = startedAt;

    public DateTimeOffset ExpiresAt { get; } = startedAt.Add(request.Duration);

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

    public AddTransactionResult TryAddTransaction(DatabaseTransaction transaction)
    {
        lock (_gate)
        {
            if (!IsActiveState(_state))
            {
                return AddTransactionResult.NotMatched;
            }

            var matchingChanges = transaction.Changes
                .Where(change =>
                    string.Equals(Request.Database, change.Database, StringComparison.OrdinalIgnoreCase) &&
                    (Request.Tables.Count == 0 || Request.Tables.Contains(change.Table)) &&
                    Request.Operations.Contains(change.Operation))
                .ToArray();

            if (matchingChanges.Length == 0)
            {
                return AddTransactionResult.NotMatched;
            }

            if (matchingChanges.Length > maxChangesPerTransaction)
            {
                return AddTransactionResult.TransactionChangeLimitReached;
            }

            if (_changeCount > maxRetainedChanges - matchingChanges.Length)
            {
                return AddTransactionResult.WatchChangeLimitReached;
            }

            var sequencedChanges = matchingChanges
                .Select((change, index) =>
                {
                    var changeSequence = _changeCount + index + 1L;
                    return change with
                    {
                        Sequence = changeSequence,
                        EventId = $"{Id}:{changeSequence}"
                    };
                })
                .ToArray();
            var transactionSequence = _transactions.Count + 1L;
            _transactions.Add(transaction with
            {
                Sequence = transactionSequence,
                Changes = sequencedChanges
            });
            _changeCount += sequencedChanges.Length;

            return _transactions.Count >= Request.MaxTransactions
                ? AddTransactionResult.MaxTransactionsReached
                : AddTransactionResult.Added;
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
                _transactions.Count,
                _changeCount,
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
            var transactions = _transactions
                .Where(transaction => transaction.Sequence > afterSequence)
                .Take(limit)
                .ToArray();

            var nextSequence = transactions.Length == 0 ? afterSequence : transactions[^1].Sequence;
            var hasMore = _transactions.Count > nextSequence;

            return new WatchEventsResponse(
                Id,
                FormatState(_state),
                transactions,
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

internal enum AddTransactionResult
{
    NotMatched,
    Added,
    MaxTransactionsReached,
    TransactionChangeLimitReached,
    WatchChangeLimitReached
}
