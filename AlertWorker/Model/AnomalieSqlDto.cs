using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AlertWorker.Model;

public class AnomalieSqlDto
{
    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = string.Empty;

    [JsonPropertyName("truck_id")]
    public string TruckId { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public string TimeStamp { get; set; } = string.Empty;

    [JsonPropertyName("engine_temp")]
    public string EngineTemp { get; set; } = string.Empty;

    public Trucks Truck { get; set; } = null!;
}
