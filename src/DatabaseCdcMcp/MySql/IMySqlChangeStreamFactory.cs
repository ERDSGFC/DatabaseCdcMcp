using DatabaseCdcMcp.Domain;

namespace DatabaseCdcMcp.MySql;

public interface IMySqlChangeStreamFactory
{
    IAsyncEnumerable<DatabaseTransaction> ReadChangesAsync(
        Func<string, string, bool> shouldCaptureTable,
        CancellationToken cancellationToken);
}
