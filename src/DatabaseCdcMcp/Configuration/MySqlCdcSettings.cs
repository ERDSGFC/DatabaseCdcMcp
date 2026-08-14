using Microsoft.Extensions.Configuration;

namespace DatabaseCdcMcp.Configuration;

public sealed record MySqlCdcSettings(
    string Hostname,
    int Port,
    string Username,
    string Password,
    long ServerId)
{
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Hostname) &&
        !string.IsNullOrWhiteSpace(Username) &&
        !string.IsNullOrWhiteSpace(Password);

    public static MySqlCdcSettings FromConfiguration(IConfiguration configuration)
    {
        return new MySqlCdcSettings(
            configuration["MYSQL_CDC_HOST"] ?? string.Empty,
            ParseInt(configuration["MYSQL_CDC_PORT"], 3306, 1, 65_535),
            configuration["MYSQL_CDC_USER"] ?? string.Empty,
            configuration["MYSQL_CDC_PASSWORD"] ?? string.Empty,
            ParseLong(configuration["MYSQL_CDC_SERVER_ID"], 6_174, 1, uint.MaxValue));
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
