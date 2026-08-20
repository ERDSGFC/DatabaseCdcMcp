using System.ComponentModel;

namespace DatabaseCdcMcp.Domain;

[Description("A committed MySQL transaction containing one or more captured row changes.")]
public sealed record DatabaseTransaction(
    [property: Description("Monotonically increasing transaction sequence within the watch, starting at 1.")]
    long Sequence,
    [property: Description("Stable transaction identifier. GTID is used when available; otherwise a Binlog position identifier is used.")]
    string TransactionId,
    [property: Description("Global transaction identifier, or null when GTID is unavailable.")]
    string? Gtid,
    [property: Description("UTC timestamp of the transaction commit event.")]
    DateTimeOffset CommittedAt,
    [property: Description("MySQL Binlog file containing the commit event.")]
    string BinlogFile,
    [property: Description("Position of the next event after the transaction commit.")]
    long CommitPosition,
    [property: Description("Original SQL statements recorded by Rows_query_log_event for this transaction, in Binlog order. Empty when binlog_rows_query_log_events is disabled or no statement was recorded.")]
    IReadOnlyList<string> Queries,
    [property: Description("Captured row changes from this transaction, preserving Binlog order.")]
    IReadOnlyList<DatabaseChange> Changes);
