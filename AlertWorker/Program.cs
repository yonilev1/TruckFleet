using AlertWorker.Data;
using AlertWorker.Handlers;
using AlertWorker.Model;
using AlertWorker.QueueReader;
using Elastic.Clients.Elasticsearch;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File(
    path: "/app/logs/rabbit_logs.log",
    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} - {Level:u3} - {Message:lj}{NewLine}{Exception}"
    ).CreateLogger();

IHost host = Host.CreateDefaultBuilder(args)
    .UseSerilog()
    .ConfigureServices((context, services) =>
    {
        var config = context.Configuration;
        var RabbitCon = config["Rabbit:ConnectionString"]!;
        var ElasticCon = config["Elastic:ConnectionString"]!;
        var sqlCon = config.GetConnectionString("DefaultConnection")!;

        services.AddDbContext<AlertDbContext>(options =>
        options.UseMySql(sqlCon, ServerVersion.AutoDetect(sqlCon)));


        services.AddSingleton<IConnectionFactory>(sp =>
        new ConnectionFactory
        {
            Uri = new Uri(RabbitCon)
        });

        services.AddSingleton<ElasticsearchClient>(sp =>
        {
            var settings = new ElasticsearchClientSettings(new Uri(ElasticCon));
            return new ElasticsearchClient(settings);
        });

        services.AddScoped<ISqlHandler, SqlHandler>();
        services.AddSingleton<IElasticHandler, ElasticHandler>();
        services.AddHostedService<RabbitReader>();
    }).Build();
using(var scope = host.Services.CreateScope())
{
    var handle = scope.ServiceProvider.GetRequiredService<AlertDbContext>();
    handle.Database.EnsureCreated();
}

using (var scope = host.Services.CreateScope())
{
    var elasticClient = scope.ServiceProvider.GetRequiredService<ElasticsearchClient>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var indexName = "anomalies-index";

    try
    {
        var exsitsResponse = await elasticClient.Indices.ExistsAsync(indexName);

        if(!exsitsResponse.Exists)
        {
            logger.LogInformation($"Index '{indexName}' does not exist. Creating with manual mapping...");

            var createResponse = await elasticClient.Indices.CreateAsync(indexName, c =>
            c.Mappings(m =>
            m.Properties<AnomaliesForElastic>(p =>
            p.Keyword(k => k.EventId)
            .Keyword(k => k.TruckId)
            .Date(d => d.TimeStamp)
            .DoubleNumber(f => f.EngineTemp))));

            if(!createResponse.IsValidResponse)
            {
                logger.LogError($"Failed to create index mapping: {createResponse.DebugInformation}");
            }
            else
            {
                logger.LogInformation("Successfully created index mapping!");
            }
        }
    }
    catch (Exception ex)
    {
        logger.LogError($"Error while setting up Elasticsearch mapping: {ex}");
    }
}
await host.RunAsync();
