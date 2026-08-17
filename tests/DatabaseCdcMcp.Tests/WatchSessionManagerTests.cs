using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.MySql;
using DatabaseCdcMcp.Tools;
using DatabaseCdcMcp.Watches;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
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

    private static WatchSessionManager CreateManager(IMySqlChangeStreamFactory factory)
    {
        return new WatchSessionManager(
            factory,
            new MySqlCdcSettings("localhost", 3306, "cdc", "secret", 6_174),
            new TestApplicationLifetime(),
            NullLogger<WatchSessionManager>.Instance);
    }

    private static DatabaseChange CreateChange(ChangeOperation operation)
    {
        return new DatabaseChange(
            0,
            string.Empty,
            "demo",
            "orders",
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
            MySqlWatchRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var change in changes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return change;
                await Task.Yield();
            }
        }
    }

    private sealed class BlockingChangeStreamFactory : IMySqlChangeStreamFactory
    {
        public async IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
            MySqlWatchRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield break;
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
