using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AlertWorker.Model;

public class Trucks
{
    [JsonPropertyName("truck_id")]
    public string TruckId { get; set; } = string.Empty;

    [JsonPropertyName("model")]
    public string Model { get; set; } = string.Empty;

    [JsonPropertyName("region")]
    public string Region { get; set; } = string.Empty;

    [JsonPropertyName("statusa")]
    public string Status { get; set; } = string.Empty;
}
