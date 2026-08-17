using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.MySql;
using Xunit;

namespace DatabaseCdcMcp.Tests;

public sealed class MySqlQueryServiceTests
{
    private readonly MySqlQueryService _service = new(
        new MySqlCdcSettings(string.Empty, 3306, string.Empty, string.Empty, 6_174));

    [Fact]
    public async Task InvalidLimitIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTableDataAsync("demo", "orders", 0));

        Assert.Contains("limit must be between", exception.Message);
    }

    [Fact]
    public async Task NegativeOffsetIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTableDataAsync("demo", "orders", 100, -1));

        Assert.Contains("offset must be zero or greater", exception.Message);
    }

    [Fact]
    public async Task MissingDatabaseIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTableSchemaAsync(string.Empty, "orders"));

        Assert.Contains("database is required", exception.Message);
    }

    [Fact]
    public async Task InvalidTableListLimitIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTablesAsync("demo", "sys_", limit: 1_001));

        Assert.Contains("limit must be between", exception.Message);
    }

    [Fact]
    public async Task NegativeTableListOffsetIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTablesAsync("demo", "sys_", limit: 100, offset: -1));

        Assert.Contains("offset must be zero or greater", exception.Message);
    }

    [Fact]
    public async Task InvalidTableNamePrefixIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTablesAsync("demo", new string('x', 65)));

        Assert.Contains("tableNamePrefix must be no longer", exception.Message);
    }

    [Fact]
    public async Task PrefixAndLikeFiltersCannotBeUsedTogether()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTablesAsync("demo", "sys_", tableNameLike: "%admin%"));

        Assert.Contains("cannot be used together", exception.Message);
    }

    [Fact]
    public async Task InvalidTableNameLikePatternIsRejectedBeforeOpeningMySql()
    {
        var exception = await Assert.ThrowsAsync<MySqlQueryException>(() =>
            _service.GetTablesAsync("demo", tableNameLike: new string('x', 257)));

        Assert.Contains("tableNameLike must be no longer", exception.Message);
    }
}
