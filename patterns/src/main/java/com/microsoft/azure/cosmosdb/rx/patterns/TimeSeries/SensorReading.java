package com.microsoft.azure.cosmosdb.rx.patterns.TimeSeries;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SensorReading {
    private String id;
    private String sensorId;
    private String siteId;
    private long unixTimeStamp;
    private double temperature;
    private double pressure;
}
