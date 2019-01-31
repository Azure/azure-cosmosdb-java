# Patterns for working with Cosmos DB

## Overview
* Cosmos DB makes it easy to build scalable applications
* These patterns are exceptions to the rule!
* Quick overview of concepts
* Vanilla (1:1 and 1:N)
* M:N

## Basic Concepts
* Documents:
  * free-form JSON
  * Must have primary key (partition key + id)
* Collections
  * Container for documents
  * Partition key definition
  * Provisioned throughput in RU/s
  * Optional indexing policy (automatic by default)
  * TTL expiration policy
  * Secondary unique key constraints
  * Collections have 1:N partitions hosting partition key ranges based on storage and throughput
  * Collections inherit consistency level, regional configuration from account
* Operations
  * CRUD: GET, POST, PUT, and DELETE
  * SQL queries (single-partition and cross-partition)
  * Intra-partition SPs (transactions)
  * Read feed (scan) and change feed
  * Every operation consumes a fixed RUs
* Thumb rules
  * In order of preference (latency and throughput)
    * GET
    * Single-partition query
    * Cross-partition query
    * Read feed (or) scan query
  * Bulk Insert (SP) > POST > PUT
  * TTL Delete > Bulk Delete (SP) > DELETE > PUT
  * Use change feed!

## Vanilla 1:1
* Let's take a gaming use case with 100s to billions of active players
  * GetPlayerById
  * AddPlayer
  * RemovePlayer
  * UpdatePlayer
* Strawman: partition key = `id`
* Only GET, POST, PUT, and DELETE
* Provisioned throughput = Sigma (RUi * Ni)
* Bonus: Bulk Inserts
* Bonus: Bulk Read (use read feed or change feed) for analytics

## Also Vanilla 1:N
* Assume a single player can play the game multiple times
* Assume the game is a single player game.
* What if we need to support lookup of game state
  * GetGameByIds(PlayerId, GameId)
  * GetGamesByPlayerId(PlayerId)
  * AddGame
  * RemoveGame
* Partition key is `playerId`
* GetGameByIds, AddGame, and RemoveGame are GET, POST, and DELETE
* GetGamesByPlayerId is a single-partition query: `SELECT * FROM c WHERE c.playerId = ‘p1’`

## What about M:N?
* Assume Multi-player gaming.
* Lookup by either `gameId` or `playerId`
  * GetGamesByPlayerId(PlayerId)
  * GetGamesByGameId(GameId)
  * AddGame
* Partition key = PlayerId is OK if mix is skewed towards Player calls (because of index on game ID)
* If mix is 50:50, then need to store two pivots of the same data by Player Id and Game Id
* Double-writes vs. change feed for keeping copies up-to-date

## Time-series data
* Ingest readings from sensors. Perform lookups by date time range
  * AddSensorReading(SensorId)
  * GetReadingsForTimeRange(StartTime, EndTime)
  * GetSensorReadingsForTimeRange(SensorId, StartTime, EndTime)
* No natural partition key. Time is an anti-pattern!
* Set partition key to Sensor ID, id to timestamp, set TTL on the time window
* Bonus: create collections for per-minute, per-hour, per-day windows based on stream aggregation on time-windows
* Bonus: create collections per hour (0-23), and set differential throughput based on the request rates