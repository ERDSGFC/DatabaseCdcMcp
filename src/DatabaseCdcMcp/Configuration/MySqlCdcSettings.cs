using Microsoft.Extensions.Configuration;

namespace DatabaseCdcMcp.Configuration;

public sealed record MySqlCdcSettings(
    string Hostname,
    int Port,
    string Username,
    string Password,
    long ServerId,
    int MaxRetainedChanges,
    int MaxChangesPerTransaction)
{
    public const int DefaultMaxRetainedChanges = 100_000;
    public const int DefaultMaxChangesPerTransaction = 10_000;

    public MySqlCdcSettings(
        string hostname,
        int port,
        string username,
        string password,
        long serverId)
        : this(
            hostname,
            port,
            username,
            password,
            serverId,
            DefaultMaxRetainedChanges,
            DefaultMaxChangesPerTransaction)
    {
    }

    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Hostname) &&
        !string.IsNullOrWhiteSpace(Username) &&
        !string.IsNullOrWhiteSpace(Password);

    public static MySqlCdcSettings FromConfiguration(IConfiguration configuration)
    {
        var maxRetainedChanges = ParsePositiveInt(
            configuration["MYSQL_CDC_MAX_RETAINED_CHANGES"],
            DefaultMaxRetainedChanges,
            "MYSQL_CDC_MAX_RETAINED_CHANGES");
        var maxChangesPerTransaction = ParsePositiveInt(
            configuration["MYSQL_CDC_MAX_CHANGES_PER_TRANSACTION"],
            DefaultMaxChangesPerTransaction,
            "MYSQL_CDC_MAX_CHANGES_PER_TRANSACTION");

        if (maxChangesPerTransaction > maxRetainedChanges)
        {
            throw new InvalidOperationException(
                "MYSQL_CDC_MAX_CHANGES_PER_TRANSACTION must not exceed " +
                "MYSQL_CDC_MAX_RETAINED_CHANGES.");
        }

        return new MySqlCdcSettings(
            configuration["MYSQL_CDC_HOST"] ?? string.Empty,
            ParseInt(configuration["MYSQL_CDC_PORT"], 3306, 1, 65_535),
            configuration["MYSQL_CDC_USER"] ?? string.Empty,
            configuration["MYSQL_CDC_PASSWORD"] ?? string.Empty,
            ParseLong(configuration["MYSQL_CDC_SERVER_ID"], 6_174, 1, uint.MaxValue),
            maxRetainedChanges,
            maxChangesPerTransaction);
    }

    private static int ParsePositiveInt(string? value, int defaultValue, string variableName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        if (!int.TryParse(value, out var parsed) || parsed < 1)
        {
            throw new InvalidOperationException(
                $"{variableName} must be a positive 32-bit integer.");
        }

        return parsed;
    }

    private static int ParseInt(string? value, int defaultValue, int min, int max)
    {
        return int.TryParse(value, out var parsed) && parsed >= min && parsed <= max
            ? parsed
            : defaultValue;
    }

    private static long ParseLong(string? value, long defaultValue, long min, long max)
    {
        return long.TryParse(value, out var parsed) && parsed >= min && parsed <= max
            ? parsed
            : defaultValue;
    }
}
