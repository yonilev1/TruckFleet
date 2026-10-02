using AlertWorker.Model;
using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AlertWorker.Handlers;

public class ElasticHandler : IElasticHandler
{
    private readonly ElasticsearchClient _elastic;
    private readonly ILogger<ElasticHandler> _logger;

    public ElasticHandler(ElasticsearchClient elastic, ILogger<ElasticHandler> logger)
    {
        _elastic = elastic;
        _logger = logger;
    }

    public async Task<bool> HandleAsync(string anomelie)
    {
        var dataArray = JsonSerializer.Deserialize<JsonElement[]>(anomelie)!;
        AnomaliesForElastic anomalieToElastic = new AnomaliesForElastic
        {
            EventId = dataArray[0].GetString()!,
            TruckId = dataArray[1].GetString()!,
            TimeStamp = dataArray[2].GetDateTime(),
            EngineTemp = dataArray[3].GetDouble()
        };
        var response = await _elastic.IndexAsync(anomalieToElastic, (IndexName)"anomalies-index");

        if (!response.IsValidResponse)
        {
            _logger.LogError($"Failed to index document in Elasticsearch: {response.DebugInformation}");
        }

        return response.IsValidResponse;
    }
}
