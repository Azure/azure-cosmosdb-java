package com.microsoft.azure.cosmosdb.rx.patterns.onetomany;

import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class Game {
    private String id;
    private String playerId;
    private double score;
    private Date startTime;
    private Date endTime;
    private String _etag;

}
