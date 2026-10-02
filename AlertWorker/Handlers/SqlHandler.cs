using AlertWorker.Data;
using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        return true;
    }
}
