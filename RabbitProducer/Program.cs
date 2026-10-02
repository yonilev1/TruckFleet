using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using Serilog;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitProducer.Consumer;
using RabbitProducer.Handler;

    
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

        services.AddSingleton<IConnectionFactory>(sp =>
        new ConnectionFactory
        {
            Uri = new Uri(RabbitCon)
        });

        services.AddScoped<RabbitHandlerAsync>();
        services.AddHostedService<KafkaConsumer>();
    }).Build();

host.Run();