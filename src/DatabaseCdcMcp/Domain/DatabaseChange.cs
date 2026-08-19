using System.ComponentModel;

namespace DatabaseCdcMcp.Domain;

/// <summary>
/// 表示一条标准化后的数据库行变化事件。
/// </summary>
/// <remarks>
/// 事件由 MySQL Binlog 读取器创建，加入监听会话时会补充序号和事件标识。
/// <see cref="Before"/> 和 <see cref="After"/> 根据操作类型保存变化前后的行数据：
/// 新增只有 <see cref="After"/>，删除只有 <see cref="Before"/>，更新同时包含两者。
/// </remarks>
/// <param name="Sequence">监听会话内的递增序号。</param>
/// <param name="EventId">事件的全局可引用标识，由监听会话和序号组合生成。</param>
/// <param name="Database">发生变化的数据库名称。</param>
/// <param name="Table">发生变化的表名称。</param>
/// <param name="Operation">行变化类型：新增、更新或删除。</param>
/// <param name="Before">变化前的列值；新增事件中为空。</param>
/// <param name="After">变化后的列值；删除事件中为空。</param>
/// <param name="Timestamp">Binlog 事件时间；无法读取时使用当前 UTC 时间。</param>
/// <param name="BinlogFile">产生该事件的 Binlog 文件名。</param>
/// <param name="BinlogPosition">该事件在 Binlog 中对应的下一事件位置。</param>
/// <param name="Gtid">该事件所属的 GTID；服务器未启用 GTID 时为空。</param>
[Description("A normalized MySQL row-change event captured from the binary log.")]
public sealed record DatabaseChange(
    [property: Description("Monotonically increasing sequence within the watch, starting at 1.")]
    long Sequence,
    [property: Description("Stable event identifier composed from the watch identifier and sequence.")]
    string EventId,
    [property: Description("Exact database name where the row changed.")]
    string Database,
    [property: Description("Exact table name where the row changed.")]
    string Table,
    [property: Description("Row operation: Insert, Update, or Delete.")]
    ChangeOperation Operation,
    [property: Description("Column values before the change; null for an insert event.")]
    IReadOnlyDictionary<string, object?>? Before,
    [property: Description("Column values after the change; null for a delete event.")]
    IReadOnlyDictionary<string, object?>? After,
    [property: Description("MySQL binlog event timestamp in UTC; current UTC time is used if the source timestamp is unavailable.")]
    DateTimeOffset Timestamp,
    [property: Description("MySQL binlog file containing the event; null when unavailable.")]
    string? BinlogFile,
    [property: Description("Position of the next event in the MySQL binlog file.")]
    long BinlogPosition,
    [property: Description("GTID associated with the event; null when GTID is disabled or unavailable.")]
    string? Gtid);
