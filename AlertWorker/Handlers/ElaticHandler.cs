using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        return true;
    }
}
