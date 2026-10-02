using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlertWorker.Handlers;

public interface IElasticHandler
{
    Task<bool> HandleAsync(string anomelie);
}
