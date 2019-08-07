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

package com.microsoft.azure.cosmosdb.internal.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import com.microsoft.azure.cosmosdb.PartitionKeyRange;
import com.microsoft.azure.cosmosdb.rx.internal.IRoutingMapProvider;
import rx.Observable;
import rx.Single;

/**
 * Provide utility functionality to route request in direct connectivity mode in the Azure Cosmos DB database service.
 */
public final class RoutingMapProviderHelper {
    private static final Range.MaxComparator<String> MAX_COMPARATOR = new Range.MaxComparator<String>();

    private static String max(String left, String right) {
        return left.compareTo(right) < 0 ? right : left;
    }

    private static <T extends Comparable<T>> boolean isSortedAndNonOverlapping(List<Range<T>> list) {
        for (int i = 1; i < list.size(); i++) {
            Range<T> previousRange = list.get(i - 1);
            Range<T> currentRange = list.get(i);

            int compareResult = previousRange.getMax().compareTo(currentRange.getMin());
            if (compareResult > 0) {
                return false;
            } else if (compareResult == 0 && previousRange.isMaxInclusive() && currentRange.isMinInclusive()) {
                return false;
            }
        }

        return true;
    }

    public static Collection<PartitionKeyRange> getOverlappingRanges(RoutingMapProvider routingMapProvider,
            String collectionSelfLink, List<Range<String>> sortedRanges) {
        if (!isSortedAndNonOverlapping(sortedRanges)) {
            throw new IllegalArgumentException("sortedRanges");
        }

        List<PartitionKeyRange> targetRanges = new ArrayList<PartitionKeyRange>();
        int currentProvidedRange = 0;
        while (currentProvidedRange < sortedRanges.size()) {
            if (sortedRanges.get(currentProvidedRange).isEmpty()) {
                currentProvidedRange++;
                continue;
            }

            Range<String> queryRange;
            if (!targetRanges.isEmpty()) {
                String left = max(targetRanges.get(targetRanges.size() - 1).getMaxExclusive(),
                        sortedRanges.get(currentProvidedRange).getMin());

                boolean leftInclusive = left.compareTo(sortedRanges.get(currentProvidedRange).getMin()) == 0
                        ? sortedRanges.get(currentProvidedRange).isMinInclusive() : false;

                queryRange = new Range<String>(left, sortedRanges.get(currentProvidedRange).getMax(), leftInclusive,
                        sortedRanges.get(currentProvidedRange).isMaxInclusive());
            } else {
                queryRange = sortedRanges.get(currentProvidedRange);
            }

            targetRanges.addAll(routingMapProvider.getOverlappingRanges(collectionSelfLink, queryRange, false));

            Range<String> lastKnownTargetRange = targetRanges.get(targetRanges.size() - 1).toRange();
            while (currentProvidedRange < sortedRanges.size()
                    && MAX_COMPARATOR.compare(sortedRanges.get(currentProvidedRange), lastKnownTargetRange) <= 0) {
                currentProvidedRange++;
            }
        }

        return targetRanges;
    }

    public static Single<List<PartitionKeyRange>> getOverlappingRanges(IRoutingMapProvider routingMapProvider,
                                                                       String resourceId, List<Range<String>> sortedRanges) {

        if (routingMapProvider == null){
            throw new IllegalArgumentException("routingMapProvider");
        }
        
        if (sortedRanges == null) {
            throw new IllegalArgumentException("sortedRanges");
        }

        if (!isSortedAndNonOverlapping(sortedRanges)) {
            throw new IllegalArgumentException("sortedRanges");
        }

        List<PartitionKeyRange> targetRanges = new ArrayList<>();
        final ListIterator<Range<String>> iterator = sortedRanges.listIterator();

        return Observable.defer(() -> {
            if (!iterator.hasNext()) {
                return Observable.empty();
            }

            Range<String> queryRange;
            Range<String> sortedRange = iterator.next();
            if (!targetRanges.isEmpty()) {
                String left = max(targetRanges.get(targetRanges.size() - 1).getMaxExclusive(),
                        sortedRange.getMin());

                boolean leftInclusive = left.compareTo(sortedRange.getMin()) == 0 && sortedRange.isMinInclusive();

                queryRange = new Range<String>(left, sortedRange.getMax(), leftInclusive,
                        sortedRange.isMaxInclusive());
            } else {
                queryRange = sortedRange;
            }

            return routingMapProvider.tryGetOverlappingRangesAsync(resourceId, queryRange, false, null)
                    .map(targetRanges::addAll)
                    .flatMap(aBoolean -> {
                        if (!targetRanges.isEmpty()) {
                            Range<String> lastKnownTargetRange = targetRanges.get(targetRanges.size() - 1).toRange();
                            while (iterator.hasNext()) {
                                Range<String> value = iterator.next();
                                if (MAX_COMPARATOR.compare(value, lastKnownTargetRange) > 0) {
                                    // Since we already moved forward on iterator to check above condition, we
                                    // go to previous when it fails so the the value is not skipped on iteration
                                    iterator.previous();
                                    break;
                                }
                            }
                        }
                        return Single.just(targetRanges);
                    }).toObservable();
        }).repeat(sortedRanges.size())
                .takeUntil(stringRange -> !iterator.hasNext())
                .last()
                .toSingle();
    }

}
