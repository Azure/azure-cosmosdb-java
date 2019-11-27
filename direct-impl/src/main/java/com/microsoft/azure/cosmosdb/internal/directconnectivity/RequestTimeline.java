package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdObjectMapper;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestRecord;

import java.time.OffsetDateTime;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents the {@link OffsetDateTime} of important events in the lifetime of a request.
 */
@JsonPropertyOrder({ "timeCreated", "timeQueued", "timeSent", "timeCompleted" })
public final class RequestTimeline {

    private final OffsetDateTime timeCreated;
    private final OffsetDateTime timeQueued;
    private final OffsetDateTime timeSent;
    private final OffsetDateTime timeCompleted;

    public RequestTimeline(final RntbdRequestRecord requestRecord) {

        checkNotNull(requestRecord, "expected non-null requestRecord");

        this.timeCreated = requestRecord.timeCreated();
        this.timeQueued = requestRecord.timeQueued();
        this.timeSent = requestRecord.timeSent();
        this.timeCompleted = requestRecord.timeCompleted();
    }

    public OffsetDateTime getTimeCompleted() {
        return this.timeCompleted;
    }

    public OffsetDateTime getTimeCreated() {
        return this.timeCreated;
    }

    public OffsetDateTime getTimeQueued() {
        return this.timeQueued;
    }

    public OffsetDateTime getTimeSent() {
        return this.timeSent;
    }

    @Override
    public String toString() {
        return RntbdObjectMapper.toString(this);
    }
}
