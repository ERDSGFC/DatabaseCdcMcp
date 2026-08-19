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
    public async Task OversizedTransactionIsRejectedWithoutRetainingPartialChanges()
    {
        var transaction = CreateTransaction(
            "tx-1",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Update));
        await using var context = await CreateContextAsync(
            new SequenceChangeStreamFactory([transaction]),
            maxRetainedChanges: 10,
            maxChangesPerTransaction: 1);

        var started = context.Manager.Start("demo", ["orders"], null, 30, 10);
        var status = await WaitUntilFinishedAsync(context.Manager, started.WatchId);

        Assert.Equal("transaction_change_limit_reached", status.FinishReason);
        Assert.Empty(context.Manager.GetEvents(started.WatchId, 0, 10).Transactions);
        Assert.Equal(0, status.TransactionCount);
        Assert.Equal(0, status.ChangeCount);
    }

    [Fact]
    public async Task WatchRetentionLimitRejectsTheNextWholeTransaction()
    {
        var first = CreateTransaction(
            "tx-1",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Update));
        var second = CreateTransaction(
            "tx-2",
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Delete));
        await using var context = await CreateContextAsync(
            new SequenceChangeStreamFactory([first, second]),
            maxRetainedChanges: 3,
            maxChangesPerTransaction: 3);

        var started = context.Manager.Start("demo", ["orders"], null, 30, 10);
        var status = await WaitUntilFinishedAsync(context.Manager, started.WatchId);

        Assert.Equal("watch_change_limit_reached", status.FinishReason);
        var retained = Assert.Single(
            context.Manager.GetEvents(started.WatchId, 0, 10).Transactions);
        Assert.Equal("tx-1", retained.TransactionId);
        Assert.Equal(2, retained.Changes.Count);
        Assert.Equal(1, status.TransactionCount);
        Assert.Equal(2, status.ChangeCount);
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
            CreateChange(
                ChangeOperation.Insert,
                "orders",
                "INSERT INTO orders (id) VALUES (1)"),
            CreateChange(
                ChangeOperation.Update,
                "customers",
                "UPDATE customers SET active = 1 WHERE id = 1")));

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
        Assert.Equal("INSERT INTO orders (id) VALUES (1)", ordersEvent.Query);
        Assert.Equal("INSERT INTO orders (id) VALUES (1)", Assert.Single(ordersTransaction.Queries));

        var customersTransaction = Assert.Single(manager.GetEvents(customersWatch.WatchId, 0, 10).Transactions);
        Assert.Equal("tx-shared", customersTransaction.TransactionId);
        var customersEvent = Assert.Single(customersTransaction.Changes);
        Assert.Equal("customers", customersEvent.Table);
        Assert.Equal(ChangeOperation.Update, customersEvent.Operation);
        Assert.Equal("UPDATE customers SET active = 1 WHERE id = 1", customersEvent.Query);
        Assert.Equal(
            "UPDATE customers SET active = 1 WHERE id = 1",
            Assert.Single(customersTransaction.Queries));
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

    private static WatchSessionManager CreateManager(
        int maxRetainedChanges = MySqlCdcSettings.DefaultMaxRetainedChanges,
        int maxChangesPerTransaction = MySqlCdcSettings.DefaultMaxChangesPerTransaction)
    {
        return new WatchSessionManager(
            new MySqlCdcSettings(
                "localhost",
                3306,
                "cdc",
                "secret",
                6_174,
                maxRetainedChanges,
                maxChangesPerTransaction),
            new TestApplicationLifetime(),
            NullLogger<WatchSessionManager>.Instance);
    }

    private static async Task<TestWatchContext> CreateContextAsync(
        IMySqlChangeStreamFactory factory,
        int maxRetainedChanges = MySqlCdcSettings.DefaultMaxRetainedChanges,
        int maxChangesPerTransaction = MySqlCdcSettings.DefaultMaxChangesPerTransaction)
    {
        var manager = CreateManager(maxRetainedChanges, maxChangesPerTransaction);
        var backgroundService = new MySqlChangeStreamBackgroundService(
            factory,
            manager,
            NullLogger<MySqlChangeStreamBackgroundService>.Instance);
        await backgroundService.StartAsync(CancellationToken.None);
        return new TestWatchContext(manager, backgroundService);
    }

    private static DatabaseChange CreateChange(
        ChangeOperation operation,
        string table = "orders",
        string? query = null)
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
            null,
            query);
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
            changes.Select(change => change.Query).OfType<string>().ToArray(),
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
