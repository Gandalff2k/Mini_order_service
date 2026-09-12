## Start

docker compose up -d --build  # to start up

docker compose up -d --build --scale outbox-worker=2 # to get two outbox workers they can run side by side safely 

pip install -e ".[dev]"   # python 3.12, needed once before running tests

pytest                    # all 110 tests, starts Postgres and Kafka containers itself

pytest -m "not kafka"     # same minus the two broker tests, much faster

check the localhost:8000/docs to work with endpoints manually

docker compose down -v # to remove the containers and their volumes 



## Architecture

PostgreSQL a source of truth for all data saving orders, products, outbox and notifications
Migrator a single entrypoint for schema application and migrations on Postgres so all dependent services wait for its successful completion
kafka a single broker in KRaft mode 
API http service working with DB
outbox-worker claims unpublished outbox rows and puts them into orders.events
consumer reads orders.events and writes notifications

A flow of events: user makes POST /orders the data is written into Postgres if no constraints were violated from there outbox-worker reads new outbox 'events' and takes FOR UPDATE SKIP LOCKED and processes them to publish into orders.events in kafka, after it gets published consumer tries to process the event either creating a notification if everything correct or putting into DLQ.

## Structure

app/api            routers, request and response schemas, dependency wiring, error mapping
app/core           rules with no infrastructure - event contract, money, backoff, domain errors
app/infra          settings, database engine and sessions, kafka clients
app/models         sqlalchemy models
app/repositories   data access, one class per aggregate
app/services       use cases, transactions begin and end here
app/workers        the two standalone processes
alembic            migrations
tests/unit         no I/O
tests/integration  real Postgres, and real Kafka where the wire format matters

api uses services, services use repositories, repositories use models.
A router never imports a repository and business logic never returns an HTTP status.

## Edge case handling 

Two orders for same product: in transaction products are locked select for update and concurrent transactions are serialized so if after first there is not enough stock the second one fails with 409.
Duplicate requests from user: unique databases constraint on customer_id and idempotency_key and instant flush of sqlalchemy to put the idempotency_key ensures that duplicate requests are impossible cause the second will get unique violation and after the completion of first returns 200. While fast select in find_replay is for usual real life cases of clicking once more after a few sec.
Multiple outbox-workers: FOR UPDATE SKIP LOCKED gives each a batch to process instead making the wait for each other or risking the double capture
Outbox message is ensured simply by being inside same Postgres transaction which is always atomic.
Publishing the event from db to kafka: marking sent after sending ensures that a crash does not lose anything it can cause duplication aka at-least-once delivery not exactly-once. Failed send does not block a full batch it gets counter raise and time change for back-off and after retries are still failed are marked failed in DB and skipped.
Message consuming: messages are keyed by order_id so one order's events are always in same partition and in correct order, autocommit off so if written but not marked in kafka will be replayed on restart and absorbed with Insert on conflict do nothing so same event_id cannot produce two notifications. If message is not correct (can not be read contract mismatch etc) straight into DLQ with payload and offset is moved, event type that is not handled is logged and skipped else for unknown mistake retry up to limit and DLQ with offset move and for mistakes like a db is down retries with backoff until successful.
Database rollback: all changes are in atomic transaction so all or nothing, and the idempotency key remains usable, stock does not change and etc.

## Why this and not that 

Migrator service resolution for problem when postgres on start is healthy and processes can try to access it but the tables do not exist yet.
Debezium just as a suggested alternative for outbox-worker overall, it does not need for update skip locked no polling but requires a kafka connect logical replication slot.
Db constraints: just ensure a last line of defence in case of py logic fail, so negative stock is not possible.
Fingerprint comparison ensures that different body with same idempotent key will get 409 instead of just getting their old order back as 200.
enable_idempotence=true deduplicates retries within a single producer session (same producer id and sequence numbers). After a worker restart the producer id is new, so a republished row is a new message to the broker so the mechanism of consumer deduplication is necessary.
Retry schedule lives inside the outbox message itself with attempts and time moved so after fail workers would not see and take it for some time.
Polling: listen/notify of Postgres I considered but did not implement, with partial index status = 'pending' the polling is not costly.
DB DLQ the row that was not published into kafka can not get into kafka DLQ it just gets marked and stays in db and workers don't claim it anymore.

## TESTs and tools 

aiokafka cause it is natively async.
Tests run actual containers to actually test behaviour and cover more than only requested cases.
110 tests, covering concurrent duplicates of one idempotency key, a duplicate arriving
after the stock already ran out, two orders locking the same products in opposite order, two publishers
draining one table, publisher retry and give up, poison message into DLQ, database down while consuming,
shutdown in the middle of a retry, and a full HTTP to notification run through a real broker.

Critical tests are mutation checked - the logic under test is deleted and the test has to go red.
It found a bug - asyncpg wraps its exceptions twice, so the unique violation recovery never matched the constraint name and a concurrent duplicate answered 500 instead of replaying the order.


## what is missing for prod grade code

No expiration and cleaning of old outbox rows and other(data overflow), a single kafka with no persistent volume(enough for test case), DLQ is never drained, no tracing metrics etc.
