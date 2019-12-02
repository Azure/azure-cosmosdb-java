# Changelog

## Unreleased
- Improved diagnostics in Direct TCP transport client ([#287](https://github.com/Azure/azure-cosmosdb-java/pull/287))
- Bumped netty.version to 4.1.42.Final and netty-tcnative.version to 2.0.26.Final to address a Direct TCP SSL issue ([#274](https://github.com/Azure/azure-cosmosdb-java/pull/274))

## 2.6.3 - 2019-10-23
- Addressed status code reporting errors that undermined retry policy:
  - `RequestRateTooLargeException.getStatusCode` now correctly returns `TOO_MANY_REQUESTS`.
  - `ServiceUnavailableException.getStatusCode` now correctly returns `SERVICE_UNAVAILABLE`.
  - `DocumentClientExceptionTest` in commons and direct-impl now verify that the correct status code is returned for all `DocumentClientException` subtypes.
  
## 2.6.2 - 2019-10-05
- Fixed query failure when setting MaxItemCount to -1 ([#261](https://github.com/Azure/azure-cosmosdb-java/issues/261)).
- Fixed a NPE bug on Partitoin split ([#267](https://github.com/Azure/azure-cosmosdb-java/pull/267)).
- Improved logging in Direct mode. 

## 2.6.1 - 2019-09-04

- Multimaster regional failover fixes for query logic ([#245](https://github.com/Azure/azure-cosmosdb-java/commit/104b4a3b30ffd7c8add01096ce6a9b6e7a3f6318))
- jackson-databind 2.9.9.3 update ([#244](https://github.com/Azure/azure-cosmosdb-java/commit/f8e60604d9e2edb0b3bd56ec7909d249de47af50))
- Direct TCP fixes (memory, perf) & metrics interface ([#242](https://github.com/Azure/azure-cosmosdb-java/commit/61562cd16845e2b249a1d91374797faa3469295f))

## 2.6.0 - 2019-08-01

- Retrieving query info using new query plan retriever ([#111](https://github.com/Azure/azure-cosmosdb-java/commit/6cdbc90386a343484f37c67bec5ec6321729b60a))
- Offset limit query support ([#234](https://github.com/Azure/azure-cosmosdb-java/commit/65606c714a6e8019e52889a8b3d49e2a5e0f6a02))

## 2.5.1 - 2019-07-31

- Fixing executeStoredProcedureInternal to use client retry policy ([#210](https://github.com/Azure/azure-cosmosdb-java/commit/eb99c15c930a7f3a413445a9e257504468329710))

## 2.4.6 - 2019-07-19

- memory improvements

## 2.5.0 - 2019-06-25

- TCP transport enabled by default
- TCP transport improvements
- Lots of testing improvements

## 2.4.5 - 2019-05-06

- Added support for Hash v2 ([#96](https://github.com/Azure/azure-cosmosdb-java/commit/f780e2d3210fbf875afcdd5e245877a49f7697ee))
- Open source direct connectivity implementation ([#94](https://github.com/Azure/azure-cosmosdb-java/commit/81a56e9506827e647253b1b6ae39f95a7aee37a3))
- Added Support for Diagnostics String 

## 2.4.4 - 2019-06-25

- misc fixes

## 2.4.3 - 2019-03-05

- Fixed resource leak issue on closing client

## 2.4.2 - 2019-03-01

- Fixed bugs in continuation token support for cross partition queries

## 2.4.1 - 2019-02-20

- Fixed some bugs in Direct mode.
- Improved logging in Direct mode.
- Improved connection management.

## 2.4.0 - 2019-02-08

- Direct GA.
- Added support for QueryMetrics.
- Changed the APIs accepting java.util.Collection for which order is important to accept java.util.List instead. Now ConnectionPolicy#getPreferredLocations(), JsonSerialization, and PartitionKey(.) accept List.

## 2.4.0-beta1 - 2019-02-04

- Added support for Direct Https.
- Changed the APIs accepting java.util.Collection for which order is important to accept java.util.List instead.
  Now ConnectionPolicy#getPreferredLocations(), JsonSerialization, and PartitionKey(.) accept List.
- Fixed a Session bug for Document query in Gateway mode.
- Upgraded dependencies (netty 0.4.20 [github #79](https://github.com/Azure/azure-cosmosdb-java/issues/79), RxJava 1.3.8).

## 2.3.1 - 2019-01-15

- Fix handling very large query responses.
- Fix resource token handling when instantiating client ([github #78](https://github.com/Azure/azure-cosmosdb-java/issues/78)).
- Upgraded vulnerable dependency jackson-databind ([github #77](https://github.com/Azure/azure-cosmosdb-java/pull/77)).

## 2.3.0 - 2018-11-29

- Fixed a resource leak bug.
- Added support for MultiPolygon
- Added support for custom headers in RequestOptions.

## 2.2.2 - 2018-11-26

- Fixed a packaging bug.

## 2.2.1 - 2018-11-05

- Fixed a NPE bug in write retry path.
- Fixed a NPE bug in endpoint management.
- Upgraded vulnerable dependencies ([github #68](https://github.com/Azure/azure-cosmosdb-java/issues/68)).
- Added support for Netty network logging for troubleshooting.

## 2.2.0 - 2018-10-04

- Added support for Multi-region write.

## 2.1.0 - 2018-09-06

- Added support for Proxy.
- Added support for resource token authorization.
- Fixed a bug in handling large partition keys ([github #63](https://github.com/Azure/azure-cosmosdb-java/issues/63)).
- Documentation improved.
- SDK restructured into more granular modules.

## 2.0.1 - 2018-08-16

- Fixed a bug for non-english locales ([github #51](https://github.com/Azure/azure-cosmosdb-java/issues/51)).
- Added helper methods for Conflict resource.

## 2.0.0 - 2018-06-20

- Replaced org.json dependency by jackson due to performance reasons and licensing ([github #29](https://github.com/Azure/azure-cosmosdb-java/issues/29)).
- Removed deprecated OfferV2 class.
- Added accessor method to Offer class for throughput content.
- Any method in Document/Resource returning org.json types changed to return a jackson object type.
- getObject(.) method of classes extending JsonSerializable changed to return a jackson ObjectNode type.
- getCollection(.) method changed to return Collection of ObjectNode.
- Removed JsonSerializable subclasses' constructors with org.json.JSONObject arg.
- JsonSerializable.toJson (SerializationFormattingPolicy.Indented) now uses two spaces for indentation.

## 1.0.2 - 2018-05-18

- Added support for Unique Index Policy.
- Added support for limiting response continuation token size in feed options.
- Added support for Partition Split in Cross Partition Query.
- Fixed a bug in Json timestamp serialization ([github #32](https://github.com/Azure/azure-cosmosdb-java/issues/32)).
- Fixed a bug in Json enum serialization.
- Fixed a bug in managing documents of 2MB size ([github #33](https://github.com/Azure/azure-cosmosdb-java/issues/33)).
- Dependency com.fasterxml.jackson.core:jackson-databind upgraded to 2.9.5 due to a bug ([jackson-databind: github #1599](https://github.com/FasterXML/jackson-databind/issues/1599))
- Dependency on rxjava-extras upgraded to 0.8.0.17 due to a bug ([rxjava-extras: github #30](https://github.com/davidmoten/rxjava-extras/issues/30)).
- The metadata description in pom file updated to be inline with the rest of documentation.
- Syntax improvement ([github #41](https://github.com/Azure/azure-cosmosdb-java/issues/41)), ([github #40](https://github.com/Azure/azure-cosmosdb-java/issues/40)).

## 1.0.1 - 2018-04-20

- Added back-pressure support in query.
- Added support for partition key range id in query.
- Changed to allow larger continuation token in request header (bugfix github #24).
- netty dependency upgraded to 4.1.22.Final to ensure JVM shuts down after main thread finishes.
- Changed to avoid passing session token when reading master resources.
- Added more examples.
- Added more benchmarking scenarios.
- Fixed java header files for proper javadoc generation.

## 1.0.0 - 2018-02-26

- Release 1.0.0 has fully end to end support for non-blocking IO using netty library in Gateway mode.
- Dependency on `azure-documentdb` SDK removed.
- Artifact id changed to `azure-cosmosdb` from `azure-documentdb-rx` in 0.9.0-rc2.
- Java package name changed to `com.microsoft.azure.cosmosdb` from `com.microsoft.azure.documentdb` in 0.9.0-rc2.

## 0.9.0-rc2 - 2018-02-26

- `FeedResponsePage` renamed to `FeedReponse`
- Some minor modifications to `ConnectionPolicy` configuration.
  All time fields and methods in ConnectionPolicy suffixed with "InMillis" to be more precise of the time unit.
- `ConnectionPolicy#setProxy()` removed.
- `FeedOptions#pageSize` renamed to
  `FeedOptions#maxItemCount`
- Release 1.0.0 deprecates 0.9.x releases.

## 0.9.0-rc1

- First release of `azure-documentdb-rx` SDK.
- CRUD Document API fully non-blocking using netty. Query async API implemented as a wrapper using blocking SDK `azure-documentdb`.


## Release and retirement dates
Microsoft will provide notification at least **12 months** in advance of retiring an SDK in order to smooth the transition to a newer/supported version.

New features and functionality and optimizations are only added to the current SDK. So it's recommended that you always upgrade to the latest SDK version as early as possible.

Any request to Cosmos DB using a retired SDK will be rejected by the service.

> [!WARNING]
> All versions **1.x** of the Async Java SDK for SQL API will be retired on **August 30, 2020**.
> 
>
<br/>

| Version | Release Date | Retirement Date |
| --- | --- | --- |
| 2.6.3 |Oct 23, 2019|--- |
| 2.6.2 |Oct 5, 2019|--- |
| 2.6.1 |Sept 4, 2019|--- |
| 2.6.0 |Aug 1, 2019|--- |
| 2.5.1 |Jul 31, 2019|--- |
| 2.4.6 |Jul 19, 2019|--- |
| 2.5.0 |Jun 25, 2019|--- |
| 2.4.5 |May 6, 2019|--- |
| 2.4.3 |Mar 5, 2019|--- |
| 2.4.2 |Mar 1, 2019|--- |
| 2.4.1 |Feb 20, 2019|--- |
| 2.4.0 |Feb 8, 2019|--- |
| 2.4.0-beta-1 |Feb 4, 2019|--- |
| 2.3.1 |Jan 15, 2019|--- |
| 2.3.0 |Nov 29, 2018|--- |
| 2.2.2 |Nov 8, 2018|--- |
| 2.2.1 |Nov 2, 2018|--- |
| 2.2.0 |September 22, 2018|--- |
| 2.1.0 |September 5, 2018|--- |
| 2.0.1 |August 16, 2018|--- |
| 2.0.0 |June 20, 2018|--- |
| 1.0.2 |May 18, 2018|August 30, 2020 |
| 1.0.1 |April 20, 2018|August 30, 2020 |
| 1.0.0 |February 27, 2018|August 30, 2020 |
