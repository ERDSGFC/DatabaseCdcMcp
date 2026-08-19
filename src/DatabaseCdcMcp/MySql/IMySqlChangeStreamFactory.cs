using DatabaseCdcMcp.Domain;

namespace DatabaseCdcMcp.MySql;

public interface IMySqlChangeStreamFactory
{
    IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
        Func<string, string, bool> shouldCaptureTable,
        CancellationToken cancellationToken);
}
