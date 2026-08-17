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
}
