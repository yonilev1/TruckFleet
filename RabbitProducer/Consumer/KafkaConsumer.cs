using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using RabbitProducer.Handler;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

namespace RabbitProducer.Consumer;

public class KafkaConsumer : BackgroundService
{
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly string _bootstrap;
    private readonly string _topic = "out_of_range";
    private readonly IConsumer<Null, string> _consumer;
    private readonly IServiceProvider _serviceProvider;

    public KafkaConsumer(ILogger<KafkaConsumer> logger, IServiceProvider serviceProvider, IConfiguration configuration)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _bootstrap = configuration["Kafka:BootstrapServer"]!;

        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrap,
            GroupId = "v2",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<Null, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var data = _consumer.Consume(stoppingToken);
                    if (data == null || data.Message.Value == null)
                        continue;
                    using(var scope = _serviceProvider.CreateScope())
                    {
                        var handlr = scope.ServiceProvider.GetRequiredService<RabbitHandlerAsync>();
                        await handlr.HandleAsync(data.Message.Value);
                        _logger.LogInformation($"Processed message: {data.Message.Value}");
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError($"Kafka consume error: {ex.Error.Reason}");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Kafka consume error: {ex}");
        }
        finally
        {
            _consumer.Close();
        }
    }
}
