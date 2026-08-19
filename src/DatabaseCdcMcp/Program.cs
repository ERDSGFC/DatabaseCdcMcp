using DatabaseCdcMcp.Configuration;
using DatabaseCdcMcp.MySql;
using DatabaseCdcMcp.Watches;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole(options =>
{
    // stdout belongs to the MCP stdio transport.
    options.LogToStandardErrorThreshold = LogLevel.Trace;
});

builder.Services.AddSingleton(MySqlCdcSettings.FromConfiguration(builder.Configuration));
builder.Services.AddSingleton<IMySqlChangeStreamFactory, MySqlCdcChangeStreamFactory>();
builder.Services.AddSingleton<MySqlQueryService>();
builder.Services.AddSingleton<WatchSessionManager>();
builder.Services.AddHostedService<MySqlChangeStreamBackgroundService>();

builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();
