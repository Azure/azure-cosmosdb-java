/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestTimeline;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestTimeoutException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.StoreResponse;
import io.micrometer.core.instrument.Timer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@JsonSerialize(using = RntbdRequestRecord.JsonSerializer.class)
public final class RntbdRequestRecord extends CompletableFuture<StoreResponse> {

    private static final AtomicReferenceFieldUpdater<RntbdRequestRecord, Stage> updater = AtomicReferenceFieldUpdater
        .newUpdater(RntbdRequestRecord.class, Stage.class, "stage");

    private final RntbdRequestArgs args;
    private final RntbdRequestTimer timer;

    private volatile Stage stage;

    private volatile OffsetDateTime timeCompleted;
    private volatile OffsetDateTime timeCreated;
    private volatile OffsetDateTime timeQueued;
    private volatile OffsetDateTime timeSent;

    public RntbdRequestRecord(final RntbdRequestArgs args, final RntbdRequestTimer timer) {

        checkNotNull(args, "expected non-null args");
        checkNotNull(timer, "expected non-null timer");

        this.timeCreated = OffsetDateTime.now();
        this.stage = Stage.CREATED;
        this.args = args;
        this.timer = timer;
    }

    // region Accessors

    public UUID activityId() {
        return this.args.activityId();
    }

    public RntbdRequestArgs args() {
        return this.args;
    }

    public long creationTime() {
        return this.args.creationTime();
    }

    public boolean expire() {
        final RequestTimeoutException error = new RequestTimeoutException(this.toString(), this.args.physicalAddress());
        BridgeInternal.setRequestHeaders(error, this.args.serviceRequest().getHeaders());
        return this.completeExceptionally(error);
    }

    public Duration lifetime() {
        return this.args.lifetime();
    }

    public Stage stage() {
        return updater.get(this);
    }

    public RntbdRequestRecord stage(final Stage value) {

        final OffsetDateTime time = OffsetDateTime.now();

        updater.updateAndGet(this, current -> {

            switch (value) {
                case QUEUED:
                    checkState(current == Stage.CREATED,
                        "expected transition from Stage.CREATED to Stage.QUEUED, not Stage.%s",
                        value);
                    this.timeQueued = time;
                    break;
                case SENT:
                    checkState(current == Stage.QUEUED,
                        "expected transition from Stage.QUEUED to Stage.SENT, not Stage.%s",
                        value);
                    this.timeSent = time;
                    break;
                case COMPLETED:
                    checkState(current == Stage.SENT,
                        "expected transition from Stage.SENT to Stage.COMPLETED, not Stage.%s",
                        value);
                    this.timeCompleted = time;
                    break;
            }

            return value;
        });

        return this;
    }

    public OffsetDateTime timeCompleted() {
        return this.timeCompleted;
    }

    public OffsetDateTime timeCreated() {
        return this.timeCreated;
    }

    public OffsetDateTime timeQueued() {
        return this.timeQueued;
    }

    public OffsetDateTime timeSent() {
        return this.timeSent;
    }

    public RequestTimeline timeline() {
        return new RequestTimeline(this);
    }

    public long transportRequestId() {
        return this.args.transportRequestId();
    }

    // endregion

    // region Methods

    public Timeout newTimeout(final TimerTask task) {
        return this.timer.newTimeout(task);
    }

    public long stop(Timer requests, Timer responses) {
        return this.args.stop(requests, responses);
    }

    @Override
    public String toString() {
        return RntbdObjectMapper.toString(this);
    }

    // endregion

    // region Types

    public enum Stage {
        CREATED, QUEUED, SENT, COMPLETED
    }

    static final class JsonSerializer extends StdSerializer<RntbdRequestRecord> {

        JsonSerializer() {
            super(RntbdRequestRecord.class);
        }

        @Override
        public void serialize(
            final RntbdRequestRecord value,
            final JsonGenerator generator,
            final SerializerProvider provider) throws IOException {

            generator.writeStartObject();

            generator.writeObjectFieldStart("status");
            generator.writeBooleanField("done", value.isDone());
            generator.writeBooleanField("cancelled", value.isCancelled());
            generator.writeBooleanField("completedExceptionally", value.isCompletedExceptionally());

            if (value.isCompletedExceptionally()) {

                try {

                    value.get();

                } catch (final ExecutionException executionException) {

                    final Throwable error = executionException.getCause();

                    generator.writeObjectFieldStart("error");
                    generator.writeStringField("type", error.getClass().getName());
                    generator.writeObjectField("value", error);
                    generator.writeEndObject();

                } catch (CancellationException | InterruptedException exception) {

                    generator.writeObjectFieldStart("error");
                    generator.writeStringField("type", exception.getClass().getName());
                    generator.writeObjectField("value", exception);
                    generator.writeEndObject();
                }
            }

            generator.writeEndObject();

            generator.writeObjectField("args", value.args);

            generator.writeStartObject("timeline");

            if (value.timeCreated() != null) {
                generator.writeStringField("created", value.timeCreated().toString());
            } else {
                generator.writeNullField("created");
            }

            if (value.timeQueued() != null) {
                generator.writeStringField("queued", value.timeQueued().toString());
            } else {
                generator.writeNullField("queued");
            }

            if (value.timeSent() != null) {
                generator.writeStringField("sent", value.timeSent().toString());
            } else {
                generator.writeNullField("sent");
            }

            if (value.timeCompleted() != null) {
                generator.writeStringField("completed", value.timeCompleted().toString());
            } else {
                generator.writeNullField("completed");
            }

            generator.writeEndObject();

            generator.writeEndObject();
        }
    }

    // endregion
}
