using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlertWorker.Handlers;

public interface ISqlHandler
{
    Task<bool> HandleAsync(string anomelie);
}
