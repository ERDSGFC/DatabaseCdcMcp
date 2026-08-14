namespace DatabaseCdcMcp.Domain;

public sealed record DatabaseChange(
    long Sequence,
    string EventId,
    string Database,
    string Table,
    ChangeOperation Operation,
    IReadOnlyDictionary<string, object?>? Before,
    IReadOnlyDictionary<string, object?>? After,
    DateTimeOffset Timestamp,
    string? BinlogFile,
    long BinlogPosition,
    string? Gtid);
