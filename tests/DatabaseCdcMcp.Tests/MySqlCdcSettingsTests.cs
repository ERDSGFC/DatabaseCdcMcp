using DatabaseCdcMcp.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace DatabaseCdcMcp.Tests;

public sealed class MySqlCdcSettingsTests
{
    [Fact]
    public void MissingValuesUseDefaults()
    {
        var settings = MySqlCdcSettings.FromConfiguration(CreateConfiguration());

        Assert.Equal(MySqlCdcSettings.DefaultMaxRetainedChanges, settings.MaxRetainedChanges);
        Assert.Equal(
            MySqlCdcSettings.DefaultMaxChangesPerTransaction,
            settings.MaxChangesPerTransaction);
    }

    [Fact]
    public void EnvironmentStyleValuesOverrideDefaults()
    {
        var settings = MySqlCdcSettings.FromConfiguration(CreateConfiguration(
            ("MYSQL_CDC_HOST", "mysql.example"),
            ("MYSQL_CDC_PORT", "3307"),
            ("MYSQL_CDC_USER", "cdc"),
            ("MYSQL_CDC_PASSWORD", "secret"),
            ("MYSQL_CDC_SERVER_ID", "7000"),
            ("MYSQL_CDC_MAX_RETAINED_CHANGES", "5000"),
            ("MYSQL_CDC_MAX_CHANGES_PER_TRANSACTION", "500")));

        Assert.Equal("mysql.example", settings.Hostname);
        Assert.Equal(3307, settings.Port);
        Assert.Equal("cdc", settings.Username);
        Assert.Equal("secret", settings.Password);
        Assert.Equal(7_000, settings.ServerId);
        Assert.Equal(5_000, settings.MaxRetainedChanges);
        Assert.Equal(500, settings.MaxChangesPerTransaction);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("invalid")]
    public void InvalidValueIsRejected(string value)
    {
        var configuration = CreateConfiguration(
            ("MYSQL_CDC_MAX_RETAINED_CHANGES", value));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            MySqlCdcSettings.FromConfiguration(configuration));

        Assert.Contains("MYSQL_CDC_MAX_RETAINED_CHANGES", exception.Message);
    }

    [Fact]
    public void PerTransactionLimitCannotExceedWatchLimit()
    {
        var configuration = CreateConfiguration(
            ("MYSQL_CDC_MAX_RETAINED_CHANGES", "100"),
            ("MYSQL_CDC_MAX_CHANGES_PER_TRANSACTION", "101"));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            MySqlCdcSettings.FromConfiguration(configuration));

        Assert.Contains("must not exceed", exception.Message);
    }

    private static IConfiguration CreateConfiguration(
        params (string Key, string Value)[] values)
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(values.ToDictionary(item => item.Key, item => item.Value)!)
            .Build();
    }
}
