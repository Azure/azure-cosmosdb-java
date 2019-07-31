package com.microsoft.azure.cosmosdb.rx.internal.query;

enum QueryFeature {
    None,
    Aggregate,
    CompositeAggregate,
    Distinct,
    GroupBy,
    MultipleAggregates,
    MultipleOrderBy,
    OffsetAndLimit,
    OrderBy,
    Top
}
