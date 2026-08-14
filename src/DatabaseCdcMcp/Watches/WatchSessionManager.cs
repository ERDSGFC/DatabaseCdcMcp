using System.Collections.Concurrent;
using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.MySql;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DatabaseCdcMcp.Watches;

public sealed class WatchSessionManager
{
    private const int MaxConcurrentSessions = 1;
    private static readonly TimeSpan MaxDuration = TimeSpan.FromMinutes(30);
    private const int MaxRetainedEvents = 10_000;

    private readonly ConcurrentDictionary<string, WatchSession> _sessions = new();
    private readonly SemaphoreSlim _sessionSlots = new(MaxConcurrentSessions, MaxConcurrentSessions);
    private readonly IMySqlChangeStreamFactory _changeStreamFactory;
    private readonly MySqlCdcSettings _settings;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly ILogger<WatchSessionManager> _logger;

    public WatchSessionManager(
        IMySqlChangeStreamFactory changeStreamFactory,
        MySqlCdcSettings settings,
        IHostApplicationLifetime applicationLifetime,
        ILogger<WatchSessionManager> logger)
    {
        _changeStreamFactory = changeStreamFactory;
        _settings = settings;
        _applicationLifetime = applicationLifetime;
        _logger = logger;
    }

    public StartWatchResponse Start(
        string database,
        IEnumerable<string>? tables,
        IEnumerable<string>? operations,
        int durationSeconds,
        int maxEvents)
    {
        if (!_settings.IsConfigured)
        {
            throw new WatchException(
                "MySQL is not configured. Set MYSQL_CDC_HOST, MYSQL_CDC_USER and MYSQL_CDC_PASSWORD before starting the MCP server.");
        }

        var request = NormalizeRequest(database, tables, operations, durationSeconds, maxEvents);

        if (!_sessionSlots.Wait(0))
        {
            throw new WatchException($"At most {MaxConcurrentSessions} watch sessions can run at the same time.");
        }

        var id = Guid.NewGuid().ToString("N");
        var startedAt = DateTimeOffset.UtcNow;
        var session = new WatchSession(id, request, startedAt);

        if (!_sessions.TryAdd(id, session))
        {
            _sessionSlots.Release();
            throw new WatchException("Failed to allocate a watch session.");
        }

        _ = RunSessionAsync(session);

        return new StartWatchResponse(
            id,
            "starting",
            startedAt,
            session.ExpiresAt,
            request.MaxEvents);
    }

    public WatchEventsResponse GetEvents(string watchId, long afterSequence, int limit)
    {
        var session = GetSession(watchId);

        if (afterSequence < 0)
        {
            throw new WatchException("afterSequence must be zero or greater.");
        }

        if (limit is < 1 or > 1_000)
        {
            throw new WatchException("limit must be between 1 and 1000.");
        }

        return session.GetEvents(afterSequence, limit);
    }

    public WatchStatusResponse GetStatus(string watchId) => GetSession(watchId).GetStatus();

    public WatchStatusResponse Stop(string watchId)
    {
        var session = GetSession(watchId);
        session.RequestStop();
        return session.GetStatus();
    }

    private async Task RunSessionAsync(WatchSession session)
    {
        using var timeoutSource = new CancellationTokenSource(session.Request.Duration);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            session.StopToken,
            timeoutSource.Token,
            _applicationLifetime.ApplicationStopping);

        try
        {
            session.MarkRunning();

            await foreach (var change in _changeStreamFactory.ReadChangesAsync(
                               session.Request,
                               linkedSource.Token))
            {
                if (!session.AddEvent(change))
                {
                    session.Complete("max_events_reached");
                    break;
                }
            }

            if (!linkedSource.IsCancellationRequested)
            {
                session.Complete("stream_ended");
            }
        }
        catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested)
        {
            session.Complete("duration_elapsed");
        }
        catch (OperationCanceledException) when (session.StopToken.IsCancellationRequested)
        {
            session.MarkStopped("stopped_by_user");
        }
        catch (OperationCanceledException) when (_applicationLifetime.ApplicationStopping.IsCancellationRequested)
        {
            session.MarkStopped("server_shutdown");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "MySQL watch {WatchId} failed", session.Id);
            session.Fail(exception);
        }
        finally
        {
            _sessionSlots.Release();
        }
    }

    private WatchSession GetSession(string watchId)
    {
        if (string.IsNullOrWhiteSpace(watchId) || !_sessions.TryGetValue(watchId, out var session))
        {
            throw new WatchException("The requested watch session does not exist.");
        }

        return session;
    }

    private static MySqlWatchRequest NormalizeRequest(
        string database,
        IEnumerable<string>? tables,
        IEnumerable<string>? operations,
        int durationSeconds,
        int maxEvents)
    {
        if (string.IsNullOrWhiteSpace(database))
        {
            throw new WatchException("database is required.");
        }

        if (durationSeconds < 1 || durationSeconds > MaxDuration.TotalSeconds)
        {
            throw new WatchException($"durationSeconds must be between 1 and {(int)MaxDuration.TotalSeconds}.");
        }

        if (maxEvents is < 1 or > MaxRetainedEvents)
        {
            throw new WatchException($"maxEvents must be between 1 and {MaxRetainedEvents}.");
        }

        var normalizedTables = new HashSet<string>(
            tables?.Where(table => !string.IsNullOrWhiteSpace(table)).Select(table => table.Trim()) ?? [],
            StringComparer.OrdinalIgnoreCase);

        var normalizedOperations = new HashSet<ChangeOperation>();
        var requestedOperations = operations?.ToArray() ?? [];

        if (requestedOperations.Length == 0)
        {
            normalizedOperations.UnionWith(Enum.GetValues<ChangeOperation>());
        }
        else
        {
            foreach (var operation in requestedOperations)
            {
                if (!Enum.TryParse<ChangeOperation>(operation, true, out var parsed))
                {
                    throw new WatchException(
                        $"Unsupported operation '{operation}'. Use insert, update or delete.");
                }

                normalizedOperations.Add(parsed);
            }
        }

        return new MySqlWatchRequest(
            database.Trim(),
            normalizedTables,
            normalizedOperations,
            TimeSpan.FromSeconds(durationSeconds),
            maxEvents);
    }
}
