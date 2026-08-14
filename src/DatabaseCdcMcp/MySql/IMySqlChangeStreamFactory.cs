using DatabaseCdcMcp.Domain;
using DatabaseCdcMcp.Watches;

namespace DatabaseCdcMcp.MySql;

public interface IMySqlChangeStreamFactory
{
    IAsyncEnumerable<DatabaseChange> ReadChangesAsync(
        MySqlWatchRequest request,
        CancellationToken cancellationToken);
}
