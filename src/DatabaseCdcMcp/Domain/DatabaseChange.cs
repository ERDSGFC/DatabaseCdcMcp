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
