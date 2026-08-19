using DatabaseCdcMcp.Watches;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DatabaseCdcMcp.MySql;

/// <summary>
/// Owns the single physical MySQL Binlog stream shared by all logical watch sessions.
/// </summary>
public sealed class MySqlChangeStreamBackgroundService(
    IMySqlChangeStreamFactory changeStreamFactory,
    WatchSessionManager sessionManager,
    ILogger<MySqlChangeStreamBackgroundService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await sessionManager.WaitForActiveSessionAsync(stoppingToken);

                try
                {
                    await foreach (var change in changeStreamFactory.ReadChangesAsync(
                                       sessionManager.ShouldCaptureTable,
                                       stoppingToken))
                    {
                        sessionManager.DispatchChange(change);
                    }

                    if (!stoppingToken.IsCancellationRequested)
                    {
                        sessionManager.CompleteActiveSessions("stream_ended");
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "The shared MySQL change stream failed.");
                    sessionManager.FailActiveSessions(exception);
                }
            }
        }
        finally
        {
            sessionManager.StopActiveSessions("server_shutdown");
        }
    }
}
