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
package com.azure.data.cosmos.internal.changefeed.implementation;

import com.azure.data.cosmos.internal.changefeed.CancellationToken;
import com.azure.data.cosmos.internal.changefeed.Lease;
import com.azure.data.cosmos.internal.changefeed.LeaseManager;
import com.azure.data.cosmos.internal.changefeed.LeaseRenewer;
import com.azure.data.cosmos.internal.changefeed.exceptions.LeaseLostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Implementation for the {@link LeaseRenewer}.
 */
class LeaseRenewerImpl implements LeaseRenewer {
    private final Logger logger = LoggerFactory.getLogger(LeaseRenewerImpl.class);
    private final LeaseManager leaseManager;
    private final Duration leaseRenewInterval;
    private Lease lease;
    private RuntimeException resultException;

    public LeaseRenewerImpl(Lease lease, LeaseManager leaseManager, Duration leaseRenewInterval)
    {
        this.lease = lease;
        this.leaseManager = leaseManager;
        this.leaseRenewInterval = leaseRenewInterval;
    }

    @Override
    public Mono<Void> run(CancellationToken cancellationToken) {
        LeaseRenewerImpl self = this;

        logger.info(String.format("Partition %s: renewer task started.", self.lease.getLeaseToken()));
        long remainingWork = this.leaseRenewInterval.toMillis();

        try {
            while (!cancellationToken.isCancellationRequested() && remainingWork > 0) {
                Thread.sleep(100);
                remainingWork -= 100;
            }
        } catch (InterruptedException ex) {
            // exception caught
            logger.info(String.format("Partition %s: renewer task stopped.", self.lease.getLeaseToken()));
            return Mono.empty();
        }

        return Mono.just(self)
            .flatMap(value -> self.renew(cancellationToken))
            .repeat(() -> {
                if (cancellationToken.isCancellationRequested()) return false;

                long remainingWorkInLoop = this.leaseRenewInterval.toMillis();

                try {
                    while (!cancellationToken.isCancellationRequested() && remainingWorkInLoop > 0) {
                        Thread.sleep(100);
                        remainingWorkInLoop -= 100;
                    }
                } catch (InterruptedException ex) {
                    // exception caught
                    logger.info(String.format("Partition %s: renewer task stopped.", self.lease.getLeaseToken()));
                    return false;
                }

                return !cancellationToken.isCancellationRequested();
            })
            .then()
            .onErrorResume(throwable -> {
                logger.error(String.format("Partition %s: renew lease loop failed.", self.lease.getLeaseToken()), throwable);
                return Mono.error(throwable);
            });
    }

    @Override
    public RuntimeException getResultException() {
        return this.resultException;
    }

    private Mono<Lease> renew(CancellationToken cancellationToken) {
        LeaseRenewerImpl self = this;

        if (cancellationToken.isCancellationRequested()) return Mono.empty();

        return self.leaseManager.renew(self.lease)
            .map(renewedLease -> {
                if (renewedLease != null) {
                    self.lease = renewedLease;
                }
                logger.info(String.format("Partition %s: renewed lease with result %s", self.lease.getLeaseToken(), renewedLease != null));
                return renewedLease;
            })
            .onErrorResume(throwable -> {
                if (throwable instanceof LeaseLostException) {
                    LeaseLostException lle = (LeaseLostException) throwable;
                    self.resultException = lle;
                    logger.error(String.format("Partition %s: lost lease on renew.", self.lease.getLeaseToken()), lle);
                    return Mono.error(lle);
                }

                logger.error(String.format("Partition %s: failed to renew lease.", self.lease.getLeaseToken()), throwable);
                return Mono.empty();
            });
    }

}
