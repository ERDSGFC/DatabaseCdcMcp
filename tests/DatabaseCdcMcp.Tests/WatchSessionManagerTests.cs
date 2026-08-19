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
        var manager = CreateManager(new BlockingChangeStreamFactory());

        var started = manager.Start("demo", null, null, 3_600, 100);

        Assert.Equal(started.StartedAt.AddHours(1), started.ExpiresAt);

        manager.Stop(started.WatchId);
        await WaitUntilFinishedAsync(manager, started.WatchId);
    }

    [Fact]
    public void DurationLongerThanOneHourIsRejected()
    {
        var manager = CreateManager(new SequenceChangeStreamFactory([]));

        var exception = Assert.Throws<WatchException>(() =>
            manager.Start("demo", null, null, 3_601, 100));

        Assert.Contains("between 1 and 3600", exception.Message);
    }

    [Fact]
    public async Task CapturedEventsAreSequencedAndCanBeReadIncrementally()
    {
        var changes = new[]
        {
            CreateChange(ChangeOperation.Insert),
            CreateChange(ChangeOperation.Update)
        };
        var manager = CreateManager(new SequenceChangeStreamFactory(changes));

        var started = manager.Start("demo", ["orders"], null, 30, 2);
        var status = await WaitUntilFinishedAsync(manager, started.WatchId);

        Assert.Equal("completed", status.State);
        Assert.Equal("max_events_reached", status.FinishReason);

        var firstPage = manager.GetEvents(started.WatchId, 0, 1);
        Assert.Single(firstPage.Events);
        Assert.Equal(1, firstPage.Events[0].Sequence);
        Assert.True(firstPage.HasMore);

        var secondPage = manager.GetEvents(started.WatchId, firstPage.NextSequence, 10);
        Assert.Single(secondPage.Events);
        Assert.Equal(2, secondPage.Events[0].Sequence);
        Assert.EndsWith(":2", secondPage.Events[0].EventId);
        Assert.False(secondPage.HasMore);
    }

    [Fact]
    public void InvalidOperationIsRejectedBeforeStartingAStream()
    {
        var manager = CreateManager(new SequenceChangeStreamFactory([]));

        var exception = Assert.Throws<WatchException>(() =>
            manager.Start("demo", null, ["truncate"], 30, 100));

        Assert.Contains("Unsupported operation", exception.Message);
    }

    [Fact]
    public async Task ActiveWatchCanBeStopped()
    {
        var manager = CreateManager(new BlockingChangeStreamFactory());
        var started = manager.Start("demo", null, null, 30, 100);

        manager.Stop(started.WatchId);
        var status = await WaitUntilFinishedAsync(manager, started.WatchId);

        Assert.Equal("stopped", status.State);
        Assert.Equal("stopped_by_user", status.FinishReason);
    }

    [Fact]
    public async Task CurrentTargetsIncludeOnlyActiveWatch()
    {
        var manager = CreateManager(new BlockingChangeStreamFactory());
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
        var manager = CreateManager(factory);

        var ordersWatch = manager.Start("demo", ["orders"], ["insert"], 30, 1);
        var customersWatch = manager.Start("demo", ["customers"], ["update"], 30, 1);

        factory.Publish(CreateChange(ChangeOperation.Delete, "orders"));
        factory.Publish(CreateChange(ChangeOperation.Insert, "orders"));
        factory.Publish(CreateChange(ChangeOperation.Update, "customers"));

        var ordersStatus = await WaitUntilFinishedAsync(manager, ordersWatch.WatchId);
        var customersStatus = await WaitUntilFinishedAsync(manager, customersWatch.WatchId);

        Assert.Equal(1, factory.StreamCount);
        Assert.Equal("max_events_reached", ordersStatus.FinishReason);
        Assert.Equal("max_events_reached", customersStatus.FinishReason);

        var ordersEvent = Assert.Single(manager.GetEvents(ordersWatch.WatchId, 0, 10).Events);
        Assert.Equal("orders", ordersEvent.Table);
        Assert.Equal(ChangeOperation.Insert, ordersEvent.Operation);

        var customersEvent = Assert.Single(manager.GetEvents(customersWatch.WatchId, 0, 10).Events);
        Assert.Equal("customers", customersEvent.Table);
        Assert.Equal(ChangeOperation.Update, customersEvent.Operation);
    }

    [Fact]
    public async Task StoppingOneWatchDoesNotStopTheSharedStreamOrOtherWatches()
    {
        var factory = new ChannelChangeStreamFactory();
        var manager = CreateManager(factory);

        var stoppedWatch = manager.Start("demo", ["orders"], null, 30, 10);
        var activeWatch = manager.Start("demo", ["customers"], null, 30, 1);

        manager.Stop(stoppedWatch.WatchId);
        factory.Publish(CreateChange(ChangeOperation.Insert, "customers"));

        var stoppedStatus = await WaitUntilFinishedAsync(manager, stoppedWatch.WatchId);
        var activeStatus = await WaitUntilFinishedAsync(manager, activeWatch.WatchId);

        Assert.Equal(1, factory.StreamCount);
        Assert.Equal("stopped_by_user", stoppedStatus.FinishReason);
        Assert.Empty(manager.GetEvents(stoppedWatch.WatchId, 0, 10).Events);
        Assert.Equal("max_events_reached", activeStatus.FinishReason);
        Assert.Single(manager.GetEvents(activeWatch.WatchId, 0, 10).Events);
    }

    private static WatchSessionManager CreateManager(IMySqlChangeStreamFactory factory)
    {
        return new WatchSessionManager(
            factory,
            new MySqlCdcSettings("localhost", 3306, "cdc", "secret", 6_174),
            new TestApplicationLifetime(),
            NullLogger<WatchSessionManager>.Instance);
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

    private sealed class SequenceChangeStreamFactory(IEnumerable<DatabaseChange> changes)
        : IMySqlChangeStreamFactory
    {
        public async IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var change in changes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (shouldCaptureTable(change.Database, change.Table))
                {
                    yield return change;
                }

                await Task.Yield();
            }
        }
    }

    private sealed class BlockingChangeStreamFactory : IMySqlChangeStreamFactory
    {
        public async IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield break;
        }
    }

    private sealed class ChannelChangeStreamFactory : IMySqlChangeStreamFactory
    {
        private readonly Channel<DatabaseChange> _changes = Channel.CreateUnbounded<DatabaseChange>();
        private int _streamCount;

        public int StreamCount => Volatile.Read(ref _streamCount);

        public void Publish(DatabaseChange change) =>
            Assert.True(_changes.Writer.TryWrite(change));

        public async IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
            Func<string, string, bool> shouldCaptureTable,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _streamCount);

            await foreach (var change in _changes.Reader.ReadAllAsync(cancellationToken))
            {
                if (shouldCaptureTable(change.Database, change.Table))
                {
                    yield return change;
                }
            }
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
