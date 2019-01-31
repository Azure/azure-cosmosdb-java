package com.microsoft.azure.cosmosdb.rx.patterns.onetoone;

import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class Player {
    private String id;
    private String name;
    private String handle;
    private double score;
    private Date startTime;
    private Date endTime;
    private String _etag;

}