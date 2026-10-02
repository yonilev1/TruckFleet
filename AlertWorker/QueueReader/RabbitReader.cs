using AlertWorker.Handlers;
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
            while(true)
            {
                try
                {
                    consumer.ReceivedAsync += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        _logger.LogInformation($" [x] Received {message}");
                        return Task.CompletedTask;
                    };
                    //bool addedToSql = 
                    await channel.BasicConsumeAsync("anomalies", autoAck: false, consumer: consumer);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Got error while reading from rabbit: {ex}");
                }
            }

        }
        catch (Exception ex)
        {
            _logger.LogError($"Got error while reading from rabbit: {ex}");
        }
    }
}
