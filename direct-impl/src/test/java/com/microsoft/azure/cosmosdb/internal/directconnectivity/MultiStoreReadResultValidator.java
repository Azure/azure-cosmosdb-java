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

package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import com.google.common.base.Predicates;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.rx.FailureValidator;
import org.apache.commons.lang3.mutable.MutableObject;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

/**
 * this is meant to be used when there reading multiple replicas for the same thing
 */
public interface MultiStoreReadResultValidator {

    static Builder create() {
        return new Builder();
    }

    void validate(List<StoreReadResult> storeReadResults);

    class Builder {
        private List<MultiStoreReadResultValidator> validators = new ArrayList<>();

        public MultiStoreReadResultValidator build() {
            return new MultiStoreReadResultValidator() {

                @SuppressWarnings({"rawtypes", "unchecked"})
                @Override
                public void validate(List<StoreReadResult> storeReadResults) {
                    for (MultiStoreReadResultValidator validator : validators) {
                        validator.validate(storeReadResults);
                    }
                }
            };
        }

        public Builder validateEachWith(StoreReadResultValidator storeReadResultValidator) {
            validators.add(new MultiStoreReadResultValidator() {

                @Override
                public void validate(List<StoreReadResult> storeReadResults) {
                    for(StoreReadResult srr: storeReadResults) {
                        storeReadResultValidator.validate(srr);
                    }
                }
            });
            return this;
        }

        public Builder validateEachWith(StoreResponseValidator storeResponseValidator) {
            validators.add(new MultiStoreReadResultValidator() {

                @Override
                public void validate(List<StoreReadResult> storeReadResults) {
                    for(StoreReadResult srr: storeReadResults) {
                        try {
                            storeResponseValidator.validate(srr.toResponse());
                        } catch (DocumentClientException e) {
                            fail(e.getMessage());
                        }
                    }
                }
            });
            return this;
        }

        public Builder withMinimumLSN(long minimumLSN) {
            this.validateEachWith(StoreReadResultValidator.create().withMinLSN(minimumLSN).build());
            return this;
        }

        public <T> Builder withAggregate(BiFunction<StoreReadResult, T, T> aggregator,
                                         T initialValue,
                                         Predicate<T> finalValuePredicate,
                                         Description description) {
            MutableObject<T> total = new MutableObject<>(initialValue);
            validators.add(new MultiStoreReadResultValidator() {

                @Override
                public void validate(List<StoreReadResult> storeReadResults) {
                    for(StoreReadResult srr: storeReadResults) {
                        total.setValue(aggregator.apply(srr, total.getValue()));
                    }

                    assertThat(finalValuePredicate.test(total.getValue()))
                            .describedAs(Description.mostRelevantDescription(description,
                                                                             String.format("actual value %s.",
                                                                                           total.getValue().toString())))
                            .isTrue();
                }
            });
            return this;
        }

        public Builder withTotalRequestCharge(double totalExpectedRC) {
            this.withAggregate((srr, v) -> srr.requestCharge + v.doubleValue(),
                               0d,
                               Predicates.equalTo(totalExpectedRC),
                               new TextDescription("total request charge is expected to be %f", totalExpectedRC));
            return this;
        }

        public Builder withNonZeroRequestCharge() {

            this.withAggregate((srr, v) -> srr.requestCharge + v.doubleValue(),
                               0d,
                               aDouble -> aDouble > 0,
                               new TextDescription("total request charge expected to be greater than 0"));
            return this;
        }

        public Builder validateEachWith(FailureValidator failureValidator) {
            validators.add(new MultiStoreReadResultValidator() {

                @Override
                public void validate(List<StoreReadResult> storeReadResults) {
                    for(StoreReadResult srr: storeReadResults) {
                        try {
                            failureValidator.validate(srr.getException());
                        } catch (DocumentClientException e) {
                            fail(e.getMessage());
                        }
                    }
                }
            });
            return this;
        }

        public Builder noFailure() {
            this.validateEachWith(StoreReadResultValidator.create().isValid().noException().build());
            return this;
        }

        public Builder withSize(int expectedNumber) {
            validators.add(new MultiStoreReadResultValidator() {

                @Override
                public void validate(List<StoreReadResult> storeReadResults) {
                    assertThat(storeReadResults).hasSize(expectedNumber);
                }
            });
            return this;
        }
    }
}
