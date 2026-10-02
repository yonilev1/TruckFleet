using AlertWorker.Data;
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

public class SqlHandler : ISqlHandler
{
    private readonly AlertDbContext _context;
    private readonly ILogger<SqlHandler> _logger;

    public SqlHandler(AlertDbContext context, ILogger<SqlHandler> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<bool> HandleAsync(string anomelie)
    {
        AnomalieSqlDto anomalieInDto = JsonSerializer.Deserialize<AnomalieSqlDto>(anomelie)!;
        Anomelies anomalieToSql = new Anomelies
        {
            EventId = anomalieInDto.EventId,
            TruckId = anomalieInDto.TruckId,
            TimeStamp = DateTime.Parse(anomalieInDto.TimeStamp),
            EngineTemp = anomalieInDto.EngineTemp
        };
        await _context.Anomelies.AddAsync(anomalieToSql);
        int rowsAffected =  await _context.SaveChangesAsync();

        if(rowsAffected > 0)
        {
            return true;
        }
        return false;
    }
}
