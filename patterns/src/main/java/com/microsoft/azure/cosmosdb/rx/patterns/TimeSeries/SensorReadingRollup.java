package com.microsoft.azure.cosmosdb.rx.patterns.TimeSeries;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SensorReadingRollup {
    private String id;
    private String sensorId;
    private String siteId;
    private int count;
    private double sumTemperature;
    private double sumPressure;
}
