using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.MySql;
using DatabaseCdcMcp.Tools;
using DatabaseCdcMcp.Watches;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading.Channels;
using Xunit;

namespace DatabaseCdcMcp.Tests;

public sealed class WatchSessionManagerTests
{
    [Fact]
    public void StartMysqlWatchDefaultsToTenMinutes()
    {
        var durationParameter = typeof(MySqlWatchTools)
            .GetMethod(nameof(MySqlWatchTools.StartMysqlWatch))!
            .GetParameters()
            .Single(parameter => parameter.Name == "durationSeconds");

        Assert.Equal(600, durationParameter.DefaultValue);
    }

    [Fact]
    public async Task OneHourDurationIsAccepted()
    {
        var manager = CreateManager();

        var started = manager.Start("demo", null, null, 3_600, 100);

        Assert.Equal(started.StartedAt.AddHours(1), started.ExpiresAt);

        manager.Stop(started.WatchId);
        await WaitUntilFinishedAsync(manager, started.WatchId);
    }

    [Fact]
    public void DurationLongerThanOneHourIsRejected()
    {
        var manager = CreateManager();

        var exception = Assert.Throws<WatchException>(() =>
            manager.Start("demo", null, null, 3_601, 100));

        Assert.Contains("between 1 and 3600", exception.Message);
    }

    [Fact]
    public async Task CapturedTransactionsAreKeptWholeAndCanBeReadIncrementally()
    {
        var transactions = new[]
        {
            CreateTransaction("tx-1", CreateChange(ChangeOperation.Insert)),
            CreateTransaction(
                "tx-2",
                CreateChange(ChangeOperation.Update),
                CreateChange(ChangeOperation.Insert))
        };
        await using var context = await CreateContextAsync(new SequenceChangeStreamFactory(transactions));
        var manager = context.Manager;

        var started = manager.Start("demo", ["orders"], null, 30, 2);
        var status = await WaitUntilFinishedAsync(manager, started.WatchId);

        Assert.Equal("completed", status.State);
        Assert.Equal("max_transactions_reached", status.FinishReason);
        Assert.Equal(2, status.TransactionCount);
        Assert.Equal(3, status.ChangeCount);

        var firstPage = manager.GetEvents(started.WatchId, 0, 1);
        var firstTransaction = Assert.Single(firstPage.Transactions);
        Assert.Equal(1, firstTransaction.Sequence);
        var firstChange = Assert.Single(firstTransaction.Changes);
        Assert.Equal(1, firstChange.Sequence);
        Assert.True(firstPage.HasMore);

        var secondPage = manager.GetEvents(started.WatchId, firstPage.NextSequence, 10);
        var secondTransaction = Assert.Single(secondPage.Transactions);
        Assert.Equal(2, secondTransaction.Sequence);
        Assert.Equal(2, secondTransaction.Changes.Count);
        Assert.Equal([2L, 3L], secondTransaction.Changes.Select(change => change.Sequence));
        Assert.EndsWith(":3", secondTransaction.Changes[1].EventId);
        Assert.False(secondPage.HasMore);
    }

    [Fact]
    public async Task TransactionLimitCountsCompleteTransactionsRatherThanRowChanges()
    {
        var transaction = CreateTransaction(
            "tx-1",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Update));
        await using var context = await CreateContextAsync(
            new SequenceChangeStreamFactory([transaction]));

        var started = context.Manager.Start("demo", ["orders"], null, 30, 1);
        var status = await WaitUntilFinishedAsync(context.Manager, started.WatchId);

        Assert.Equal("max_transactions_reached", status.FinishReason);
        Assert.Equal(1, status.TransactionCount);
        Assert.Equal(2, status.ChangeCount);
        var captured = Assert.Single(
            context.Manager.GetEvents(started.WatchId, 0, 10).Transactions);
        Assert.Equal(2, captured.Changes.Count);
    }

    [Fact]
    public void OversizedTransactionIsRejectedWithoutRetainingPartialChanges()
    {
        var session = CreateSession(maxRetainedChanges: 10, maxChangesPerTransaction: 1);
        var transaction = CreateTransaction(
            "tx-1",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Update));

        var result = session.TryAddTransaction(transaction);

        Assert.Equal(AddTransactionResult.TransactionChangeLimitReached, result);
        Assert.Empty(session.GetEvents(0, 10).Transactions);
        Assert.Equal(0, session.GetStatus().TransactionCount);
        Assert.Equal(0, session.GetStatus().ChangeCount);
    }

    [Fact]
    public void WatchRetentionLimitRejectsTheNextWholeTransaction()
    {
        var session = CreateSession(maxRetainedChanges: 3, maxChangesPerTransaction: 3);
        var first = CreateTransaction(
            "tx-1",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Update));
        var second = CreateTransaction(
            "tx-2",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Delete));

        Assert.Equal(AddTransactionResult.Added, session.TryAddTransaction(first));
        Assert.Equal(
            AddTransactionResult.WatchChangeLimitReached,
            session.TryAddTransaction(second));

        var retained = Assert.Single(session.GetEvents(0, 10).Transactions);
        Assert.Equal("tx-1", retained.TransactionId);
        Assert.Equal(2, retained.Changes.Count);
        Assert.Equal(1, session.GetStatus().TransactionCount);
        Assert.Equal(2, session.GetStatus().ChangeCount);
    }

    [Fact]
    public void InvalidOperationIsRejectedBeforeStartingAStream()
    {
        var manager = CreateManager();

        var exception = Assert.Throws<WatchException>(() =>
            manager.Start("demo", null, ["truncate"], 30, 100));

        Assert.Contains("Unsupported operation", exception.Message);
    }

    [Fact]
    public async Task ActiveWatchCanBeStopped()
    {
        var manager = CreateManager();
        var started = manager.Start("demo", null, null, 30, 100);

        manager.Stop(started.WatchId);
        var status = await WaitUntilFinishedAsync(manager, started.WatchId);

        Assert.Equal("stopped", status.State);
        Assert.Equal("stopped_by_user", status.FinishReason);
    }

    [Fact]
    public async Task CurrentTargetsIncludeOnlyActiveWatch()
    {
        var manager = CreateManager();
        var started = manager.Start("demo", ["orders", "customers"], ["insert", "update"], 30, 100);

        var targets = manager.GetCurrentTargets();

        var target = Assert.Single(targets.Watches);
        Assert.Equal(started.WatchId, target.WatchId);
        Assert.Equal("demo", target.Database);
        Assert.False(target.AllTables);
        Assert.Equal(["customers", "orders"], target.Tables);
        Assert.Equal(["insert", "update"], target.Operations);

        manager.Stop(started.WatchId);
        await WaitUntilFinishedAsync(manager, started.WatchId);

        Assert.Empty(manager.GetCurrentTargets().Watches);
    }

    [Fact]
    public async Task MultipleWatchesShareOneStreamAndReceiveOnlyMatchingEvents()
    {
        var factory = new ChannelChangeStreamFactory();
        await using var context = await CreateContextAsync(factory);
        var manager = context.Manager;

        var ordersWatch = manager.Start("demo", ["orders"], ["insert"], 30, 1);
        var customersWatch = manager.Start("demo", ["customers"], ["update"], 30, 1);

        factory.Publish(CreateTransaction("tx-delete", CreateChange(ChangeOperation.Delete, "orders")));
        factory.Publish(CreateTransaction(
            "tx-shared",
            CreateChange(ChangeOperation.Insert, "orders"),
            CreateChange(ChangeOperation.Update, "customers")));

        var ordersStatus = await WaitUntilFinishedAsync(manager, ordersWatch.WatchId);
        var customersStatus = await WaitUntilFinishedAsync(manager, customersWatch.WatchId);

        Assert.Equal(1, factory.StreamCount);
        Assert.Equal("max_transactions_reached", ordersStatus.FinishReason);
        Assert.Equal("max_transactions_reached", customersStatus.FinishReason);

        var ordersTransaction = Assert.Single(manager.GetEvents(ordersWatch.WatchId, 0, 10).Transactions);
        Assert.Equal("tx-shared", ordersTransaction.TransactionId);
        var ordersEvent = Assert.Single(ordersTransaction.Changes);
        Assert.Equal("orders", ordersEvent.Table);
        Assert.Equal(ChangeOperation.Insert, ordersEvent.Operation);

        var customersTransaction = Assert.Single(manager.GetEvents(customersWatch.WatchId, 0, 10).Transactions);
        Assert.Equal("tx-shared", customersTransaction.TransactionId);
        var customersEvent = Assert.Single(customersTransaction.Changes);
        Assert.Equal("customers", customersEvent.Table);
        Assert.Equal(ChangeOperation.Update, customersEvent.Operation);
    }

    [Fact]
    public async Task StoppingOneWatchDoesNotStopTheSharedStreamOrOtherWatches()
    {
        var factory = new ChannelChangeStreamFactory();
        await using var context = await CreateContextAsync(factory);
        var manager = context.Manager;

        var stoppedWatch = manager.Start("demo", ["orders"], null, 30, 10);
        var activeWatch = manager.Start("demo", ["customers"], null, 30, 1);

        manager.Stop(stoppedWatch.WatchId);
        factory.Publish(CreateTransaction("tx-1", CreateChange(ChangeOperation.Insert, "customers")));

        var stoppedStatus = await WaitUntilFinishedAsync(manager, stoppedWatch.WatchId);
        var activeStatus = await WaitUntilFinishedAsync(manager, activeWatch.WatchId);

        Assert.Equal(1, factory.StreamCount);
        Assert.Equal("stopped_by_user", stoppedStatus.FinishReason);
        Assert.Empty(manager.GetEvents(stoppedWatch.WatchId, 0, 10).Transactions);
        Assert.Equal("max_transactions_reached", activeStatus.FinishReason);
        Assert.Single(manager.GetEvents(activeWatch.WatchId, 0, 10).Transactions);
    }

    [Fact]
    public async Task ImmediatelyCompletedStreamWaitsForAnotherWatchBeforeRestarting()
    {
        var factory = new CompletingChangeStreamFactory();
        await using var context = await CreateContextAsync(factory);

        var started = context.Manager.Start("demo", null, null, 30, 10);
        var status = await WaitUntilFinishedAsync(context.Manager, started.WatchId);

        await Task.Delay(50);

        Assert.Equal("completed", status.State);
        Assert.Equal("stream_ended", status.FinishReason);
        Assert.Equal(1, factory.StreamCount);

        var second = context.Manager.Start("demo", null, null, 30, 10);
        var secondStatus = await WaitUntilFinishedAsync(context.Manager, second.WatchId);

        Assert.Equal("stream_ended", secondStatus.FinishReason);
        Assert.Equal(2, factory.StreamCount);
    }

    private static WatchSessionManager CreateManager()
    {
        return new WatchSessionManager(
            new MySqlCdcSettings("localhost", 3306, "cdc", "secret", 6_174),
            new TestApplicationLifetime(),
            NullLogger<WatchSessionManager>.Instance);
    }

    private static async Task<TestWatchContext> CreateContextAsync(IMySqlChangeStreamFactory factory)
    {
        var manager = CreateManager();
        var backgroundService = new MySqlChangeStreamBackgroundService(
            factory,
            manager,
            NullLogger<MySqlChangeStreamBackgroundService>.Instance);
        await backgroundService.StartAsync(CancellationToken.None);
        return new TestWatchContext(manager, backgroundService);
    }

    private static DatabaseChange CreateChange(ChangeOperation operation, string table = "orders")
    {
        return new DatabaseChange(
            0,
            string.Empty,
            "demo",
            table,
            operation,
            null,
            new Dictionary<string, object?> { ["id"] = 1 },
            DateTimeOffset.UtcNow,
            "mysql-bin.000001",
            123,
            null);
    }

    private static WatchSession CreateSession(
        int maxTransactions = 10,
        int maxRetainedChanges = 100,
        int maxChangesPerTransaction = 100)
    {
        var request = new MySqlWatchRequest(
            "demo",
            new HashSet<string>(["orders"], StringComparer.OrdinalIgnoreCase),
            new HashSet<ChangeOperation>(Enum.GetValues<ChangeOperation>()),
            TimeSpan.FromMinutes(1),
            maxTransactions);

        return new WatchSession(
            "watch-1",
            request,
            DateTimeOffset.UtcNow,
            maxRetainedChanges,
            maxChangesPerTransaction);
    }

    private static DatabaseTransaction CreateTransaction(
        string transactionId,
        params DatabaseChange[] changes)
    {
        return new DatabaseTransaction(
            0,
            transactionId,
            transactionId.StartsWith("gtid-", StringComparison.Ordinal) ? transactionId : null,
            DateTimeOffset.UtcNow,
            "mysql-bin.000001",
            200,
            changes);
    }

    private static async Task<WatchStatusResponse> WaitUntilFinishedAsync(
        WatchSessionManager manager,
        string watchId)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (!timeout.IsCancellationRequested)
        {
            var status = manager.GetStatus(watchId);
            if (status.State is not ("starting" or "running"))
            {
                return status;
            }

            await Task.Delay(10, timeout.Token);
        }

        throw new TimeoutException("The watch did not finish in time.");
    }

    private sealed class SequenceChangeStreamFactory(IEnumerable<DatabaseTransaction> transactions)
        : IMySqlChangeStreamFactory
    {
        public async IAsyncEnumerable<DatabaseTransaction> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var transaction in transactions)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (transaction.Changes.Any(change => shouldCaptureTable(change.Database, change.Table)))
                {
                    yield return transaction;
                }

                await Task.Yield();
            }
        }
    }

    private sealed class BlockingChangeStreamFactory : IMySqlChangeStreamFactory
    {
        public async IAsyncEnumerable<DatabaseTransaction> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield break;
        }
    }

    private sealed class ChannelChangeStreamFactory : IMySqlChangeStreamFactory
    {
        private readonly Channel<DatabaseTransaction> _transactions = Channel.CreateUnbounded<DatabaseTransaction>();
        private int _streamCount;

        public int StreamCount => Volatile.Read(ref _streamCount);

        public void Publish(DatabaseTransaction transaction) =>
            Assert.True(_transactions.Writer.TryWrite(transaction));

        public async IAsyncEnumerable<DatabaseTransaction> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _streamCount);

            await foreach (var transaction in _transactions.Reader.ReadAllAsync(cancellationToken))
            {
                if (transaction.Changes.Any(change => shouldCaptureTable(change.Database, change.Table)))
                {
                    yield return transaction;
                }
            }
        }
    }

    private sealed class CompletingChangeStreamFactory : IMySqlChangeStreamFactory
    {
        private int _streamCount;

        public int StreamCount => Volatile.Read(ref _streamCount);

        public async IAsyncEnumerable<DatabaseTransaction> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _streamCount);
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield break;
        }
    }

    private sealed class TestWatchContext(
        WatchSessionManager manager,
        MySqlChangeStreamBackgroundService backgroundService) : IAsyncDisposable
    {
        public WatchSessionManager Manager { get; } = manager;

        public async ValueTask DisposeAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await backgroundService.StopAsync(timeout.Token);
            backgroundService.Dispose();
        }
    }

    private sealed class TestApplicationLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => CancellationToken.None;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
        }
    }
}
