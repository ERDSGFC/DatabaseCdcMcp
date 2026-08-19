using System.Collections.Concurrent;
using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.MySql;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DatabaseCdcMcp.Watches;

/// <summary>
/// 管理当前 MySQL 监听会话的生命周期和内存事件队列。
/// </summary>
public sealed class WatchSessionManager
{
    private const int MaxConcurrentSessions = 32;
    private static readonly TimeSpan MaxDuration = TimeSpan.FromHours(1);
    private const int MaxRetainedEvents = 100_000;

    private readonly ConcurrentDictionary<string, WatchSession> _sessions = new();
    private readonly SemaphoreSlim _sessionSlots = new(MaxConcurrentSessions, MaxConcurrentSessions);
    private readonly Lock _changeStreamGate = new();
    private Task? _changeStreamTask;

    private readonly IMySqlChangeStreamFactory _changeStreamFactory;
    private readonly MySqlCdcSettings _settings;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly ILogger<WatchSessionManager> _logger;

    /// <summary>
    /// 创建一个对应已配置 MySQL 数据源的监听会话管理器。
    /// </summary>
    /// <param name="changeStreamFactory">用于打开 Binlog 数据流的工厂。</param>
    /// <param name="settings">已读取并校验的 MySQL 连接配置。</param>
    /// <param name="applicationLifetime">用于在服务关闭时停止监听的 Host 生命周期对象。</param>
    /// <param name="logger">用于记录后台监听任务异常的日志对象。</param>
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

    /// <summary>
    /// 启动一个有时限和数量上限的监听，并立即返回监听标识。
    /// </summary>
    /// <param name="database">要监听的数据库。</param>
    /// <param name="tables">可选的表过滤条件；为空表示监听所有表。</param>
    /// <param name="operations">可选的新增、更新和删除操作过滤条件。</param>
    /// <param name="durationSeconds">监听的最长持续时间。</param>
    /// <param name="maxEvents">最多保留的事件数量。</param>
    /// <returns>新创建的监听会话信息。</returns>
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

        _ = RunSessionLifetimeAsync(session);
        EnsureChangeStreamStarted();

        return new StartWatchResponse(
            id,
            "starting",
            startedAt,
            session.ExpiresAt,
            request.MaxEvents);
    }

    /// <summary>
    /// 读取指定序号之后的一页事件。
    /// </summary>
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

    /// <summary>
    /// 返回监听会话的当前状态和统计信息。
    /// </summary>
    public WatchStatusResponse GetStatus(string watchId) => GetSession(watchId).GetStatus();

    /// <summary>
    /// 返回当前仍在运行的监听目标；已完成的监听不会出现在结果中。
    /// </summary>
    public WatchTargetsResponse GetCurrentTargets()
    {
        var watches = _sessions.Values
            .Select(session => session.GetTarget())
            .Where(target => target is not null)
            .Select(target => target!)
            .OrderBy(target => target.WatchId, StringComparer.Ordinal)
            .ToArray();

        return new WatchTargetsResponse(watches);
    }

    /// <summary>
    /// 请求取消活动监听，并返回当前状态。
    /// </summary>
    public WatchStatusResponse Stop(string watchId)
    {
        var session = GetSession(watchId);
        session.RequestStop();
        return session.GetStatus();
    }

    private async Task RunSessionLifetimeAsync(WatchSession session)
    {
        try
        {
            session.MarkRunning();
            await session.Completion.WaitAsync(
                session.Request.Duration,
                _applicationLifetime.ApplicationStopping);
        }
        catch (TimeoutException)
        {
            session.Complete("duration_elapsed");
        }
        catch (OperationCanceledException) when (_applicationLifetime.ApplicationStopping.IsCancellationRequested)
        {
            session.MarkStopped("server_shutdown");
        }
        finally
        {
            _sessionSlots.Release();
        }
    }

    private void EnsureChangeStreamStarted()
    {
        lock (_changeStreamGate)
        {
            if (_changeStreamTask is null || _changeStreamTask.IsCompleted)
            {
                _changeStreamTask = RunChangeStreamAsync();
            }
        }
    }

    private async Task RunChangeStreamAsync()
    {
        try
        {
            await foreach (var change in _changeStreamFactory.ReadChangesAsync(
                               ShouldCaptureTable,
                               _applicationLifetime.ApplicationStopping))
            {
                DispatchChange(change);
            }

            if (!_applicationLifetime.ApplicationStopping.IsCancellationRequested)
            {
                CompleteActiveSessions("stream_ended");
            }
        }
        catch (OperationCanceledException) when (_applicationLifetime.ApplicationStopping.IsCancellationRequested)
        {
            StopActiveSessions("server_shutdown");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "The shared MySQL change stream failed.");
            FailActiveSessions(exception);
        }
        finally
        {
            lock (_changeStreamGate)
            {
                _changeStreamTask = null;

                // A watch may have been added while the previous stream was exiting.
                if (!_applicationLifetime.ApplicationStopping.IsCancellationRequested &&
                    _sessions.Values.Any(session => session.GetTarget() is not null))
                {
                    _changeStreamTask = RunChangeStreamAsync();
                }
            }
        }
    }

    private bool ShouldCaptureTable(string database, string table) =>
        _sessions.Values.Any(session => session.MatchesTarget(database, table));

    private void DispatchChange(DatabaseChange change)
    {
        foreach (var session in _sessions.Values)
        {
            if (session.TryAddEvent(change, out var reachedLimit) && reachedLimit)
            {
                session.Complete("max_events_reached");
            }
        }
    }

    private void CompleteActiveSessions(string reason)
    {
        foreach (var session in _sessions.Values)
        {
            session.Complete(reason);
        }
    }

    private void StopActiveSessions(string reason)
    {
        foreach (var session in _sessions.Values)
        {
            session.MarkStopped(reason);
        }
    }

    private void FailActiveSessions(Exception exception)
    {
        foreach (var session in _sessions.Values)
        {
            session.Fail(exception);
        }
    }

    /// <summary>
    /// 查找监听会话；找不到时抛出面向调用方的校验错误。
    /// </summary>
    private WatchSession GetSession(string watchId)
    {
        if (string.IsNullOrWhiteSpace(watchId) || !_sessions.TryGetValue(watchId, out var session))
        {
            throw new WatchException("The requested watch session does not exist.");
        }

        return session;
    }

    /// <summary>
    /// 在后台任务启动前校验并标准化工具参数。
    /// </summary>
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
