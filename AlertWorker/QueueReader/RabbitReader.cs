using AlertWorker.Handlers;
using Elastic.Clients.Elasticsearch.Inference;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace AlertWorker.QueueReader;

public class RabbitReader :BackgroundService
{
    private readonly ILogger<RabbitReader> _logger;
    private readonly IConnectionFactory _factory;
    private IConnection? _connection;
    private readonly IElasticHandler _elastic;
    private readonly IServiceProvider _serviceProvider;

    public RabbitReader(ILogger<RabbitReader> logger,
        IConnectionFactory factory,
        IElasticHandler elastic,
        IServiceProvider serviceProvider)
    {
        _logger = logger;
        _factory = factory;
        _elastic = elastic;
        _serviceProvider = serviceProvider;
    }

    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {

            if (_connection == null || !_connection.IsOpen)
            {
                _connection = await _factory.CreateConnectionAsync();
            }
            using var channel = await _connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(
                queue: "anomalies",
                durable: true,
                exclusive: false,
                autoDelete: false);


            var consumer = new AsyncEventingBasicConsumer(channel);
            try
            {
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        _logger.LogInformation($" [x] Received {message}");

                        bool addedToElastic = await _elastic.HandleAsync(message);
                        bool addedToSql = false;

                        using (var scope = _serviceProvider.CreateScope())
                        {
                            var sqlHandler = scope.ServiceProvider.GetRequiredService<ISqlHandler>();
                            addedToSql = await sqlHandler.HandleAsync(message);
                        }

                        if (addedToElastic && addedToSql)
                        {
                            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                        else
                        {
                            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Error processing message {ea.DeliveryTag}: {ex}");

                        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                    }
                };

                await channel.BasicConsumeAsync("anomalies", autoAck: false, consumer: consumer);
                await Task.Delay(Timeout.Infinite, stoppingToken);


            }
            catch (Exception ex)
            {
                _logger.LogError($"Got error while setting up RabbitMQ consumer: {ex}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Got error while setting up RabbitMQ consumer: {ex}");
        }
    }
}
