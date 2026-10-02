using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using System.Text;

namespace RabbitProducer.Handler;

public class RabbitHandlerAsync
{
    private readonly ILogger<RabbitHandlerAsync> _logger;
    private readonly IConnectionFactory _factory;
    private IConnection? _connection;

    public RabbitHandlerAsync(ILogger<RabbitHandlerAsync> logger, IConnectionFactory factory)
    {
        _logger = logger;
        _factory = factory;
    }

    public async Task HandleAsync(string anomalie)
    {
        try
        {

            if(_connection == null || !_connection.IsOpen)
            {
                _connection = await _factory.CreateConnectionAsync();
            }
            using var channel = await _connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(
                queue: "anomalies",
                durable: true,
                exclusive: false,
                autoDelete: false);

            var body = Encoding.UTF8.GetBytes(anomalie);

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: "anomalies",
                body: body);
            _logger.LogInformation($"sent anomalie to rabbit {anomalie}");
        }
        catch(Exception ex)
        {
            _logger.LogError($"Got error while writing to rabbit: {ex}");
        }
    }
}
