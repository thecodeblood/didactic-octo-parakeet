# Multi-Region Ride-Hailing Platform — System Design

---

## 1. Introduction & Requirements

A ride-hailing platform connects riders with nearby drivers in real time. The system must handle real-time location ingestion at massive scale, sub-second driver matching, dynamic surge pricing, full trip lifecycle management, and reliable payment processing — all across multiple geographic regions.

### 1.1 Functional Requirements
- Ingest real-time driver location updates (1–2 updates/second per driver)
- Allow riders to request rides with pickup, destination, tier, and payment method
- Match riders with nearby drivers within **≤1s dispatch latency (p95)**
- Support dynamic surge pricing based on supply/demand per H3 geo-cell
- Manage full trip lifecycle: `SEARCHING → MATCHED → ARRIVED → IN_PROGRESS → COMPLETED`
- Calculate fares, generate receipts, and process payments via external PSPs
- Send push/SMS notifications for all key ride state changes
- Provide admin tooling: feature flags, kill-switches, observability

### 1.2 Non-Functional Requirements

| Requirement | Target | Notes |
| --- | --- | --- |
| Dispatch latency | ≤ 1s p95 | Geo query → lock → notify driver |
| End-to-end accept | ≤ 3s p95 | Includes 15s driver response window |
| Availability | 99.95% | ~4.4 hours downtime/year |
| Concurrent drivers | 300,000 | Online and sending GPS |
| Ride requests | 60k/min (~1k/sec) | Metro-concentrated peak |
| Location throughput | ~500k writes/sec | Global peak across all regions |
| Multi-region | 3 active regions | ap-south-1, ap-southeast-1, eu-west-1 |
| API idempotency | All mutating APIs | Mobile clients on flaky networks |
| Compliance | PCI DSS, GDPR, DPDP | Payment and PII data protection |

---

## 2. Scale Analysis & Capacity Planning

Before designing the architecture we derive concrete throughput numbers. Every downstream decision — Kafka partition counts, Redis cluster sizing, pod counts — is anchored to these figures.

### 2.1 Derived Capacity Numbers

| Component | Calculation | Result |
| --- | --- | --- |
| Kafka: driver.location.updates | 500k msg/s ÷ ~1k msg/s per partition | 512 partitions, LZ4 compression |
| Redis Geo Index | 300k driver keys × ~200 bytes | ~60 MB hot data → 3-node cluster, 16 GB each |
| WebSocket connections | 300k drivers + 300k active riders | 20 Connection Service pods × 50k conn each |
| Matching Service | 1k req/s × 50ms avg processing = 50 concurrent | 10 pods, auto-scale to 30 on peak |
| Trip DB (CockroachDB) | 20M trips/day ≈ 230 writes/sec + reads | 3-node cluster, REGIONAL BY ROW |
| Location History (Cassandra) | 500k writes/sec (batched cold path) | 6-node cluster, RF=3 |
| Surge Calculator | ~800 H3 cells/city × 3 cities, 30s cycle | Single-threaded per region, 2 replicas |

### 2.2 Key Trade-offs Made

| Decision | Chosen | Why Not Alternative |
| --- | --- | --- |
| Geo Index storage | Redis in-memory (GEOADD) | PostGIS: durable but 10ms+ at 500k writes/s; durability not needed — rebuilds from Kafka in <60s |
| Realtime transport | WebSocket (bidirectional) | SSE: receive-only, needs separate upload channel for driver GPS |
| Trip database | CockroachDB REGIONAL BY ROW | Postgres+replicas: manual cross-region failover; CRDB handles it natively |
| Event publishing | Outbox pattern → Kafka | Dual-write: DB succeeds but Kafka publish fails = phantom events; Outbox is atomic |
| Geo cell standard | Uber H3 hexagonal (res-7) | Geohash: rectangular cells with non-uniform area globally; H3 has uniform ~5km² cells |
| Match trigger | Kafka async (ride.requested) | Sync RPC: couples Ride Service to Matching Service; Kafka survives restarts |

---

## 3. APIs & Events

All APIs are served over HTTPS/2. Authentication via JWT in the `Authorization` header. User identity (`rider_id`, `driver_id`) is **ALWAYS** extracted from the JWT — never trusted from request body. All mutating endpoints require an `Idempotency-Key` header. Timestamps are always generated server-side.

### 3.1 Rider APIs

**`GET /rides/estimate` — Fare Estimate**

Query: `pickup_lat`, `pickup_lng`, `dest_lat`, `dest_lng`, `tier`

Response 200:
```json
{
  "fare_id":          "fare_abc123",
  "estimated_fare":   { "amount": 245.50, "currency": "INR" },
  "surge_multiplier": 1.4,
  "surge_active":     true,
  "distance_km":      12.3,
  "eta_seconds":      1380,
  "expires_at":       "2026-03-10T10:05:00Z"
}
```
> **Security:** `fare_id` is server-generated and stored in DB. The client **NEVER** sends fare amount — it is always re-read server-side on `POST /rides` to prevent price manipulation.

**`POST /rides` — Create Ride Request**

Headers: `Idempotency-Key: <uuid>`
```json
{
  "fare_id":           "fare_abc123",
  "pickup_location":   { "lat": 12.9716, "lng": 77.5946 },
  "destination":       { "lat": 12.8456, "lng": 77.6603 },
  "ride_tier":         "standard",
  "payment_method_id": "pm_xyz"
}
```
Response 201:
```json
{ "ride_id": "uuid", "status": "SEARCHING", "created_at": "2026-03-10T10:00:00Z" }
```

**`GET /rides/{ride_id}` — Ride Status**
```json
{
  "ride_id": "uuid", "status": "MATCHED",
  "driver": { "name":"Arjun S.", "vehicle":"Swift DL01AB1234", "rating":4.8, "eta_seconds":240 },
  "live_tracking_enabled": true
}
```

**`POST /rides/{ride_id}/cancel`**
```json
// Request
{ "reason": "RIDER_CANCELLED" }
// Response 200
{ "ride_id":"...", "status":"CANCELLED", "cancellation_fee":{"amount":25,"currency":"INR"} }
```

### 3.2 Driver APIs

**`POST /drivers/location` — Update GPS** *(REST fallback; primary is WebSocket)*

Headers: `Idempotency-Key: <uuid>` | Body: `{ "lat": 12.97, "lng": 77.59 }` | Response: `204 No Content`

> `driver_id` extracted from JWT. `client_ts` validated ±30s from server time.

**`POST /drivers/rides/{ride_id}/accept`**

Headers: `Idempotency-Key: drv-accept-{ride_id}-{driver_id}`

Response 200: `{ ride_id, status: MATCHED, pickup: { lat, lng, address }, navigation_url }`

**`POST /drivers/rides/{ride_id}/reject`**

Response 200: `{ ride_id, status: SEARCHING }`

### 3.3 Trip APIs

**`PATCH /trips/{trip_id}/status` — Lifecycle Transitions**

Headers: `Idempotency-Key: trip_{id}-{action}` | Body: `{ "action": "ARRIVED" }`

Valid transitions: `MATCHED→ARRIVED`, `ARRIVED→IN_PROGRESS`, `IN_PROGRESS→COMPLETED`

**`POST /trips/{trip_id}/end` — End Trip**
```json
{
  "trip_id": "uuid", "fare": { "amount": 320, "currency": "INR" },
  "distance_km": 8.2, "duration_minutes": 15,
  "receipt_url": "https://receipts.example.com/uuid"
}
```

### 3.4 Payment APIs

**`POST /payments` — Initiate Payment** *(internal, called by Trip Service on END_TRIP)*

> **Security:** Amount is **NEVER** accepted from client. Payment Service reads fare from `trips` table using `trip_id`.

Headers: `Idempotency-Key: SHA256(trip_id + ':charge_1')` *(deterministic, not random)*
```json
{ "trip_id": "uuid", "payment_method_id": "pm_xyz" }
```

### 3.5 Admin APIs

| Endpoint | Method | Description |
| --- | --- | --- |
| `POST /admin/feature-flags` | POST | Create or update a feature flag with value + scope (per-region) |
| `POST /admin/services/{service}/disable` | POST | Kill-switch: stops accepting new requests for a service |
| `GET /admin/metrics` | GET | Returns real-time SLO metrics: dispatch p95, Kafka lag, Redis memory |
| `POST /admin/surge/{region}/override` | POST | Manually set or freeze surge multiplier for a region |

### 3.6 WebSocket Events (Server → Client)

| Event Type | Direction | Trigger | Key Payload Fields |
| --- | --- | --- | --- |
| `ride_offer` | Server→Driver | Matching Service selects driver | trip_id, pickup, destination, fare, expires_in_seconds |
| `offer_expired` | Server→Driver | 15s timeout | trip_id |
| `ride_matched` | Server→Rider | Driver accepts | driver details, vehicle, ETA, tracking_token |
| `driver_location` | Server→Rider | Every GPS update during trip | lat, lng, heading, eta_to_pickup_seconds |
| `driver_arrived` | Server→Rider | ARRIVED transition | trip_id, wait_timer_seconds |
| `trip_started` | Server→Both | IN_PROGRESS transition | trip_id, metering_started_at |
| `trip_completed` | Server→Both | COMPLETED transition | actual_fare, receipt_url |
| `payment_result` | Server→Rider | PSP callback processed | status, amount, receipt_url |

### 3.7 Kafka Topics & Event Schemas

**Standard Event Envelope**
```json
{
  "event_id":       "uuid-v4",
  "event_type":     "trip.state.changed",
  "schema_version": "1.0",
  "timestamp":      "2026-03-10T10:00:05Z",
  "region":         "ap-south-1",
  "correlation_id": "uuid-v4",
  "payload":        { }
}
```

**Topic Definitions**

| Topic | Partition Key | Partitions | Retention | Producers | Consumers |
| --- | --- | --- | --- | --- | --- |
| `driver.location.updates` | driver_id | 512 | 2h | Location Svc | Geo Index Writer, Cassandra Writer |
| `ride.requested` | rider_id | 128 | 7d | Ride Svc (Outbox) | Matching Svc, Surge Consumer, Analytics |
| `trip.state.changed` | trip_id | 256 | 30d | Trip Svc (Outbox) | Notification Svc, Payment Svc, Analytics |
| `payment.initiated` | rider_id | 64 | 90d | Trip Svc (Outbox) | Payment Svc (exactly-once CG) |
| `payment.result` | trip_id | 64 | 90d | Payment Svc | Trip Svc, Notification Svc, Reconciliation |
| `surge.multiplier.updated` | h3_cell | 32 | 24h | Surge Calculator | Surge Cache Writer, Analytics |
| `payment.dlq` | rider_id | 16 | 180d | Payment Svc (on failure) | Ops Dashboard, Manual Review |

**`trip.state.changed` — Payload Schema**
```json
{ "trip_id":"uuid", "rider_id":"uuid", "driver_id":"uuid",
  "from_state":"SEARCHING", "to_state":"MATCHED",
  "driver_eta_seconds":240, "transitioned_at":"2026-03-10T10:00:05Z" }
```

**`driver.location.updates` — Payload Schema**
```json
{ "driver_id":"uuid", "lat":28.6139, "lng":77.2090,
  "heading":270, "speed_kmh":42, "accuracy_m":5,
  "client_ts":"2026-03-10T10:00:01.234Z" }
```

**`payment.initiated` — Payload Schema**
```json
{ "payment_id":"uuid", "trip_id":"uuid", "rider_id":"uuid",
  "amount_inr":245.50, "currency":"INR",
  "payment_method_id":"pm_xyz", "psp":"razorpay",
  "idempotency_key":"SHA256(trip_id+:charge_1)" }
```

---

## 4. Data Model — Full ERD

Three persistence tiers: **CockroachDB** for transactional data (strong consistency), **Redis** for hot operational state (rebuilds from Kafka), **Cassandra** for append-only location history.

### 4.1 CockroachDB Tables

**Table: `riders`**

| Column | Type | Constraints / Notes |
| --- | --- | --- |
| rider_id | UUID | PRIMARY KEY, default gen_random_uuid() |
| name | VARCHAR(100) | NOT NULL |
| phone_e164 | VARCHAR(20) | UNIQUE NOT NULL — AES-256 encrypted (PII); index on SHA-256 hash for lookup |
| email | VARCHAR(255) | UNIQUE — AES-256 encrypted (PII) |
| default_payment_method_id | UUID | FK → payment_methods.id NULLABLE |
| region | VARCHAR(20) | NOT NULL — home region e.g. ap-south-1 |
| gdpr_consent_ts | TIMESTAMPTZ | NULLABLE — GDPR/DPDP consent record |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() |
| deleted_at | TIMESTAMPTZ | NULLABLE — soft delete for right-to-erasure |

**Table: `drivers`**

| Column | Type | Constraints / Notes |
| --- | --- | --- |
| driver_id | UUID | PRIMARY KEY |
| name | VARCHAR(100) | NOT NULL |
| phone_e164 | VARCHAR(20) | UNIQUE NOT NULL — AES-256 encrypted |
| vehicle_plate | VARCHAR(20) | UNIQUE NOT NULL — encrypted |
| tier_capability | VARCHAR[] | Array of supported tiers: [standard, xl, comfort] |
| rating | NUMERIC(3,2) | DEFAULT 5.0 |
| acceptance_rate | NUMERIC(5,4) | 0.0–1.0, used in matching rank score |
| status | ENUM | OFFLINE \| AVAILABLE \| EN_ROUTE \| ON_TRIP |
| region | VARCHAR(20) | NOT NULL — home region |
| version | INTEGER | NOT NULL DEFAULT 0 — optimistic concurrency lock for double-dispatch |

**Table: `trips` — Core Entity**

| Column | Type | Constraints / Notes |
| --- | --- | --- |
| trip_id | UUID | PRIMARY KEY |
| rider_id | UUID | NOT NULL FK → riders.rider_id |
| driver_id | UUID | NULLABLE FK → drivers.driver_id — NULL until MATCHED |
| status | ENUM | SEARCHING\|MATCHED\|ARRIVED\|IN_PROGRESS\|COMPLETED\|CANCELLED NOT NULL |
| pickup_lat / pickup_lng | DOUBLE PRECISION | NOT NULL |
| pickup_h3_cell | VARCHAR(20) | NOT NULL — H3 cell for surge lookup + analytics |
| dest_lat / dest_lng | DOUBLE PRECISION | NOT NULL |
| tier | ENUM | standard \| xl \| comfort NOT NULL |
| surge_multiplier | NUMERIC(4,2) | NOT NULL DEFAULT 1.0 — snapshot at booking |
| estimated_fare | NUMERIC(10,2) | NOT NULL |
| actual_fare | NUMERIC(10,2) | NULLABLE — set on COMPLETED |
| metered_distance_km | NUMERIC(8,3) | NULLABLE — accumulated during IN_PROGRESS |
| region | VARCHAR(20) | NOT NULL — CockroachDB regional routing key |
| idempotency_key | UUID | UNIQUE NOT NULL — prevents duplicate trip creation on retry |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() |
| matched_at / arrived_at / started_at / completed_at / cancelled_at | TIMESTAMPTZ | Lifecycle timestamps |

```sql
-- Indexes on trips:
CREATE INDEX idx_trips_rider  ON trips (rider_id, created_at DESC);
CREATE INDEX idx_trips_driver ON trips (driver_id, status) WHERE driver_id IS NOT NULL;
CREATE INDEX idx_trips_active ON trips (region, status) WHERE status IN ('SEARCHING','MATCHED','ARRIVED','IN_PROGRESS');
CREATE INDEX idx_trips_idem   ON trips (idempotency_key);
```

**Table: `trip_outbox` — Transactional Outbox Pattern**

| Column | Type | Constraints / Notes |
| --- | --- | --- |
| id | BIGSERIAL | PRIMARY KEY |
| trip_id | UUID | NOT NULL FK → trips.trip_id |
| event_id | UUID | NOT NULL UNIQUE — Kafka message key (dedup) |
| event_type | VARCHAR(60) | NOT NULL — e.g. trip.state.changed |
| kafka_topic | VARCHAR(100) | NOT NULL |
| partition_key | VARCHAR(100) | NOT NULL |
| payload | JSONB | NOT NULL — full event envelope |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() |
| published_at | TIMESTAMPTZ | NULLABLE — set after Kafka ACK |
| failed_attempts | SMALLINT | NOT NULL DEFAULT 0 |

> **Why Outbox?** The Outbox `INSERT` is in the **SAME** database transaction as the trip state `UPDATE`. This guarantees: an event is published if and only if the state change committed. Without this, a crash between DB write and Kafka publish creates phantom or missing events.

**Table: `payments`**

| Column | Type | Constraints / Notes |
| --- | --- | --- |
| payment_id | UUID | PRIMARY KEY |
| trip_id | UUID | NOT NULL UNIQUE FK → trips.trip_id |
| rider_id | UUID | NOT NULL FK → riders.rider_id |
| payment_method_id | UUID | NOT NULL FK → payment_methods.id |
| amount | NUMERIC(10,2) | NOT NULL — read from trips.actual_fare, NEVER from client |
| currency | CHAR(3) | NOT NULL DEFAULT INR |
| status | ENUM | PENDING \| PROCESSING \| SUCCEEDED \| FAILED \| REFUNDED |
| idempotency_key | VARCHAR(100) | UNIQUE NOT NULL — SHA256(trip_id+':charge_1') |
| psp_name | VARCHAR(30) | NULLABLE — razorpay \| stripe \| paytm |
| psp_transaction_id | VARCHAR(100) | UNIQUE NULLABLE — PSP reference |
| attempt_count | SMALLINT | NOT NULL DEFAULT 0 |
| settled_at | TIMESTAMPTZ | NULLABLE |
| failure_reason | VARCHAR(255) | NULLABLE |

**Table: `payment_methods`**

| Column | Type | Constraints / Notes |
| --- | --- | --- |
| id | UUID | PRIMARY KEY |
| rider_id | UUID | NOT NULL FK → riders.rider_id |
| type | ENUM | CARD \| UPI \| WALLET \| CASH |
| psp_token | VARCHAR(255) | NOT NULL — PCI-tokenized; raw PAN NEVER stored |
| masked_display | VARCHAR(20) | NOT NULL — e.g. ****4242 for UI only |
| is_default | BOOLEAN | NOT NULL DEFAULT FALSE |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() |

### 4.2 Redis Key Schema

| Key Pattern | Type | Value / Schema | TTL |
| --- | --- | --- | --- |
| `geo:{region}:{gh2}` | Sorted Set (GEO) | member=driver_id, score=geo-encoded lat/lng | None (continuous update) |
| `driver:loc:{driver_id}` | Hash | lat, lng, geohash, heading, speed, status, updated_at | 45s — stale detection |
| `driver:lock:{driver_id}` | String | trip_id (lock owner) — SET NX EX 15 | 15s — offer window |
| `surge:{h3_cell}` | String | Multiplier float e.g. 1.80 | 60s — safety fallback |
| `surge:demand:{h3_cell}` | Sorted Set | member=rider_id, score=unix_ts (sliding window) | 5 min |
| `conn:{user_id}` | String | connection_service_pod_id | 30s + heartbeat |
| `trip:offer:{trip_id}` | Hash | candidates, current_index, attempt_count, status, expires_at | 5 min |
| `idem:{svc}:{key}` | String | JSON response body | 24h |

### 4.3 Cassandra Schema — Location History

```sql
CREATE TABLE location_history (
  driver_id   UUID,
  bucket      DATE,       -- partition: one per driver per day
  recorded_at TIMESTAMP,  -- clustering key DESC
  trip_id     UUID,       -- NULL when driver not on trip
  lat         DOUBLE,
  lng         DOUBLE,
  speed_kmh   FLOAT,
  heading     SMALLINT,
  PRIMARY KEY ((driver_id, bucket), recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
  AND default_time_to_live = 7776000;  -- 90 days
```

### 4.4 Entity Relationships

| Entity A | Relationship | Entity B | Notes |
| --- | --- | --- | --- |
| Rider | 1 : N | Trip | A rider has many trips over time |
| Driver | 1 : N | Trip | A driver has many trips; each trip has at most one assigned driver |
| Trip | 1 : 1 | Payment | Each completed trip has exactly one payment record |
| Rider | 1 : N | PaymentMethod | A rider can save multiple payment methods |
| Trip | 1 : N | TripOutbox | One outbox event per state transition |
| Driver | 1 : N | LocationHistory (Cassandra) | Historical GPS trail |
| Driver | 1 : 1 (hot) | DriverLocation (Redis) | Current position only, rebuilt from Kafka on crash |

---

## 5. High-Level Design (HLD)

The architecture is organized into five layers. All synchronous calls stay within a single region on the hot path. Kafka is the asynchronous backbone between all layers. The API Gateway is the sole client entry point.

### 5.1 Component Responsibilities

| Service | Layer | Responsibility | Primary Storage |
| --- | --- | --- | --- |
| API Gateway | Infrastructure | TLS, JWT auth, rate limiting, GeoDNS routing, idempotency-key injection | — |
| Ride Service | Core | Fare estimation, trip creation, surge multiplier application | CockroachDB: trips, fares |
| Trip Service | Core | State machine (SEARCHING→COMPLETED), outbox publishing, fare metering | CockroachDB: trips + outbox |
| Matching Service | Core | Geo query, candidate ranking, offer FSM (offer→accept/decline/timeout) | Redis: locks, offer state |
| Location Service | Core | Ingests GPS; hot path→Redis Geo Index; cold path→Kafka→Cassandra | Redis (hot), Cassandra (cold) |
| Connection Service | Core | 600k+ WebSocket connections; routes pushes via Connection Registry | Redis: conn registry |
| Surge Calculator | Support | Reads supply+demand per H3 cell every 30s; computes multiplier | Redis: surge cache |
| Payment Service | Support | PSP orchestration, retries, reconciliation, DLQ handling | CockroachDB: payments |
| Notification Service | Support | Consumes trip.state.changed; FCM/APNs push + SMS fallback | — |
| Routing Service | Support | Road-network graph; Dijkstra/A* for distance+ETA | In-memory graph |
| Admin Service | Ops | Feature flags, kill-switches, per-region config, observability | CockroachDB: config |

<img width="1565" height="580" alt="Image" src="https://github.com/user-attachments/assets/60566dfc-4472-4438-ac6b-8d5e88f6173f" />

### 5.2 Request Data Flow — Ride Request to Match

| Step | From → To | Protocol |
| :--- | :--- | :--- |
| 1. Rider requests ride | Rider App → API Gateway | HTTPS/REST |
| 2. Route to Ride Service | API Gateway → Ride Service | Internal HTTP/2 |
| 3. Fetch surge multiplier | Ride Service → Redis (Surge Cache) | Redis GET |
| 4. Write trip (SEARCHING) | Ride Service → CockroachDB + Outbox | SQL transaction |
| 5. Publish `ride.requested` | Outbox Reader → Kafka | Kafka produce |
| 6. Trigger matching | Ride Service → Matching Service | Kafka async |
| 7. Geo query | Matching Service → Redis GeoSearch | GEORADIUS |
| 8. Acquire driver lock | Matching Service → Redis (SETNX) | Redis atomic |
| 9. Push offer to driver | Matching Svc → Connection Svc → Driver App | WebSocket |
| 10. Driver accepts | Driver App → API Gateway → Trip Service | HTTPS/REST |
| 11. Update trip (MATCHED) | Trip Service → CockroachDB + Outbox | SQL transaction |
| 12. Notify rider | Notification Service ← Kafka | WebSocket push |

**SLO:** Steps 1–9 complete in ~100ms p50 — well within 1s p95 dispatch budget. Steps 1–12 complete in ~130ms p50 — within 3s p95 E2E budget. The 15s driver response window dominates p95; mitigated by immediately trying the next candidate on timeout.

### 5.3 Driver Location Ingestion Flow

<img width="1593" height="463" alt="Image" src="https://github.com/user-attachments/assets/23d5a166-aefc-4d57-9fce-2efcdeacced4" />

1. Driver App sends GPS over persistent WebSocket every 1–2s: `{ driver_id, lat, lng, heading, speed, client_ts }`.
2. Connection Service validates `client_ts` (±30s from server time) and forwards to Location Service via Kafka topic `driver.location.updates`.
3. **Location Service HOT PATH**: `GEOADD` to Redis Geo Index. Apply latest-timestamp-wins — silently drop if `client_ts` < stored timestamp. Sub-millisecond write.
4. **Location Service COLD PATH**: forward raw event to Kafka. Separate consumer batches 500 events and bulk-writes to Cassandra (`driver_id` + `date` partition key).
5. **TRIP RELAY**: if driver is on active trip (checked via Redis `driver:status:{id}`), look up rider's Connection Service pod in Connection Registry and push live location to rider's WebSocket.

---

## 6. Low-Level Design — Dispatch & Matching

The Matching Service is the most latency-critical and consistency-critical component. It must find the best available driver within 1s p95 and guarantee no driver receives two simultaneous offers.

### 6.1 Redis Geo Index — Exact Commands

```bash
# Key structure:
# geo:{region}:{geohash2prefix}  →  Sorted Set (GEO)
# driver:loc:{driver_id}         →  Hash (metadata + TTL=45s)

# On each GPS update:
GEOADD geo:ap-south-1:dr {lng} {lat} {driver_id}
HSET driver:loc:{driver_id} lat {lat} lng {lng} geohash {gh} updated_at {ts}
EXPIRE driver:loc:{driver_id} 45

# Proximity query (9-cell geohash search):
GEORADIUSBYMEMBER geo:ap-south-1:dr {driver_id} 3000 m WITHCOORD WITHDIST COUNT 20 ASC
```

### 6.2 Matching Algorithm — Step by Step

| Step | Action | Detail |
| --- | --- | --- |
| 1 | Receive ride request | Kafka consumer reads `ride.requested`; extracts `pickup_lat`, `pickup_lng`, `ride_tier` |
| 2 | Compute H3 cell | `h3.geoToH3(pickup_lat, pickup_lng, resolution=9)` → cell_id for ~0.1 km² precision |
| 3 | Initial geo query | `GEORADIUSBYMEMBER` on rider's geohash cell + 8 neighbours (9-cell search), radius=3km, `WITHCOORD WITHDIST COUNT 20 ASC` |
| 4 | Filter candidates | Remove: status ≠ AVAILABLE; `driver:loc` TTL expired (stale); incompatible tier |
| 5 | Expand if needed | If candidates < 3, double radius to 6km, repeat. Cap at 15km. Empty → notify rider no drivers available. |
| 6 | Rank candidates | `score = w1×distance + w2×(1/driver_rating) + w3×acceptance_rate_penalty`. Sort ascending (lower = better). |
| 7 | Acquire driver lock | `SET driver:lock:{driver_id} {trip_id} NX EX 15` — atomic. If nil returned, driver already locked, skip. |
| 8 | Send offer | Matching Svc → Connection Svc → Driver App WebSocket: offer payload |
| 9a | Driver accepts | `POST /drivers/rides/{id}/accept` → Trip Service with optimistic lock |
| 9b | Driver declines | `DEL driver:lock:{driver_id}`; advance to next candidate |
| 9c | Timeout (15s) | Lock TTL expires automatically; Matching Service polls Redis for expiry; advances index |
| 10 | Confirm match | `UPDATE trips SET status=MATCHED WHERE trip_id=? AND status=SEARCHING`; publish outbox event |

### 6.3 Double-Dispatch Prevention — Two Layers

**Layer 1 — Redis Distributed Lock (pre-DB)**
```bash
# Atomically claim driver BEFORE sending offer
SET driver:lock:{driver_id} {trip_id} NX EX 15
# NX  = only set if Not eXists (atomic test-and-set)
# EX 15 = auto-release after 15 seconds (offer window)
# Returns OK  → lock acquired
# Returns nil → driver already locked by another request → skip to next candidate
```

**Layer 2 — Optimistic Concurrency on DB (post-accept)**
```sql
BEGIN;
  -- Claim driver atomically using version counter
  UPDATE drivers
    SET status = 'EN_ROUTE', version = version + 1
    WHERE driver_id = :driver_id
      AND status   = 'AVAILABLE'
      AND version  = :expected_version;
  -- 0 rows affected → version mismatch → another request won → ROLLBACK

  UPDATE trips
    SET status = 'MATCHED', driver_id = :driver_id
    WHERE trip_id = :trip_id AND status = 'SEARCHING';
COMMIT;
```

> **Why both?** Redis lock prevents the DB race from being attempted — reduces contention. DB optimistic lock is the authoritative guarantee — handles the rare case where the Redis lock is lost during Redis failover within the 15s window. Version counter avoids timestamp clock-skew issues.

### 6.4 Offer FSM State Machine

| Event | Current State | Next State | Action |
| --- | --- | --- | --- |
| Offer sent | SEARCHING | OFFERING[i] | SET driver:lock, push WebSocket offer, start 15s timer |
| Driver accepts | OFFERING[i] | MATCHED | DB transaction (optimistic lock), publish trip.state.changed |
| Driver declines | OFFERING[i] | OFFERING[i+1] | DEL driver:lock[i], advance index, offer next candidate |
| 15s timeout | OFFERING[i] | OFFERING[i+1] | Lock TTL expired, advance index, offer next candidate |
| All candidates exhausted | OFFERING[n] | EXPANDING | Double geo radius, re-query, reset candidate list |
| 60s overall timeout | Any | FAILED | Publish trip.no_match event, notify rider |

### 6.5 Stale Driver Handling

- Each `driver:loc:{driver_id}` key has TTL=45s. If 45s pass without a GPS update, the key expires.
- The filter step checks whether `driver:loc` exists before including a candidate. Expired key = excluded.
- A background sweeper runs every 30s: `ZREM geo:{region}:{gh2} {driver_id}` for all drivers whose `loc` key has expired but whose geo sorted-set entry still exists.
- The DB `driver.status` column is set to `OFFLINE` by Location Service after 60s without a heartbeat — this is the authoritative record; Redis is the fast operational index.

### 6.6 Crash Recovery

1. Redis `driver:lock` TTL expires after 15s — driver is automatically freed. No manual intervention needed.
2. Kafka consumer group re-assigns the partition to another Matching Service instance.
3. New instance reads `trip:offer:{trip_id}` state from Redis and continues from `current_index`.
4. If `trip:offer` key also evicted: query DB for `SEARCHING` trips created in last 60s and restart offer FSM.

### 6.7 Dispatch Service Internal Components

**MatchingService** — Coordinates the overall matching process.
```java
class MatchingService {
    GeoIndex geoIndex;
    RankingService rankingService;
    DispatchLockManager lockManager;
    OfferService offerService;

    matchRide(RideRequest request);
}
```

**GeoIndex** — Handles spatial driver lookup. Backed by Redis.
```java
class GeoIndex {
    List<DriverLocation> findNearbyDrivers(Location pickup, int radius);
}
```

**RankingService** — Ranks candidate drivers by proximity, rating, and acceptance history.
```java
class RankingService {
    List<Driver> rankDrivers(List<Driver> candidates, RideRequest request);
}
```

**DispatchLockManager** — Ensures drivers cannot receive multiple simultaneous offers.
```java
class DispatchLockManager {
    boolean tryLockDriver(UUID driverId);
    void releaseDriver(UUID driverId);
}
```

**OfferService** — Handles communication with drivers via the Connection Service WebSocket.
```java
class OfferService {
    void sendRideOffer(UUID driverId, RideRequest request);
    void handleAccept(UUID driverId, UUID tripId);
    void handleDecline(UUID driverId, UUID tripId);
}
```

---

## 7. Surge Pricing Design

Surge pricing dynamically multiplies the base fare when demand in a geographic cell significantly exceeds available driver supply.

<img width="1373" height="678" alt="Image" src="https://github.com/user-attachments/assets/5ea839f9-ca51-4b08-9b4d-6ea5e4736566" />

### 7.1 H3 Geo-Cell Partitioning

| Parameter | Value / Rationale |
| --- | --- |
| Cell library | Uber H3 (resolution 7) |
| Approx cell area | ~5.16 km² per cell |
| Update frequency | Every 30 seconds per cell |
| Max surge multiplier | 5.0x (configurable per region via Admin Service feature flag) |
| Surge activation threshold | demand_ratio > 1.5 |
| Smoothing | 70% new value + 30% previous (prevents jarring UI jumps) |

### 7.2 Multiplier Formula

```
demand_ratio = active_requests_5min / available_drivers_in_cell

multiplier = 1.0                                    if demand_ratio < 1.5
multiplier = 1.0 + (ratio - 1.5) × 2.0             if 1.5 ≤ ratio < 2.0
multiplier = 2.0 + (ratio - 2.0) × 1.5             if 2.0 ≤ ratio < 3.0
multiplier = min(max_cap, prev × 0.3 + new × 0.7)  smoothed blend
```

### 7.3 Demand Signal Pipeline

1. Rider confirms ride → Ride Service publishes `ride.requested` to Kafka.
2. Surge Consumer reads `ride.requested`. For each event: `ZADD surge:demand:{h3_cell} {unix_ts} {rider_id}`.
3. Surge Calculator counts demand: `ZCOUNT surge:demand:{h3_cell} {now-300} {now}` (rolling 5-min window).
4. Supply is read from Redis Geo Index: `GEORADIUSBYMEMBER` count per H3 cell = available drivers.
5. Multiplier is computed and written: `HSET surge:{h3_cell} multiplier {value}; EXPIRE 60`.

### 7.4 Fare Estimate Integration

1. Rider calls `GET /rides/estimate` — Ride Service converts pickup lat/lng to H3 cell (resolution 7).
2. `Redis GET surge:{h3_cell}` — single sub-ms read.
3. On cache miss: default to 1.0x; async recalculation triggered.
4. Final fare = `base_fare × surge_multiplier`. Multiplier shown explicitly to rider ('1.4x surge active').
5. On `POST /rides` confirmation: surge multiplier is re-fetched server-side and locked into `trips.surge_multiplier`. **Price protection:** if new multiplier > estimate_multiplier × 1.1, the estimate multiplier is honoured.

> **Security:** Surge multiplier is always read from server-side Redis cache. It is **never** accepted from the client.

---

## 8. Payments Orchestration

Payments are asynchronous, idempotent, and resilient to PSP outages. The trip transitions to `COMPLETED` before payment is confirmed — the rider is not blocked.

### 8.1 Payment Flow

1. Trip transitions to `COMPLETED` → Trip Service writes atomically: `UPDATE trips` + `INSERT trip_outbox` (event: `payment.initiated`).
2. Outbox Relay publishes `payment.initiated` to Kafka. Payment Service consumes (exactly-once consumer group).
3. Payment Service reads amount from `trips.actual_fare` — **NEVER** from the event payload (security).
4. Constructs idempotency key: `SHA256(trip_id + ':charge_1')`. Deterministic — same key on any retry.
5. Calls PSP (Razorpay/Stripe) with idempotency key. PSP guarantees exactly-once charge for identical keys within 24h.
6. Payment Service writes result to `payments` table and publishes `payment.result` to Kafka.
7. Notification Service consumes `payment.result`: sends receipt push to rider.

### 8.2 Retry Schedule for PSP Failures

| Attempt | Delay | Action on Failure |
| --- | --- | --- |
| 1 (immediate) | 0s | PSP call on first consume |
| 2 | 30s | Republish to payment.initiated with attempt=2 header |
| 3 | 5 min | Republish with attempt=3 |
| 4 | 30 min | Republish with attempt=4 |
| 5 | 2 hours | Final attempt |
| Exhausted | — | Move to payment.dlq; alert on-call; push+email to rider with manual payment link |

### 8.3 Three-Layer Payment Idempotency

- **Layer 1 — Deterministic key**: `SHA256(trip_id+':charge_1')` computed identically on any pod/retry. Not random — a random key would create a new charge on each retry.
- **Layer 2 — PSP idempotency**: PSP is called with the same key on every retry. PSP guarantees: same key = same result, no double charge.
- **Layer 3 — DB unique constraint**: `payments.idempotency_key` has `UNIQUE` constraint. If two Payment Service instances race, the second `INSERT` fails and the first response is returned.

### 8.4 Nightly Reconciliation

1. Query all trips `COMPLETED` yesterday from CockroachDB.
2. For each trip: if `payment.status = SUCCEEDED` but no PSP confirmation recorded, query PSP settlement API.
3. For each trip: if `payment.status = PENDING`, trigger retry or flag for manual review.
4. Compare total revenue (`SUM actual_fare`) with PSP settlement report total.
5. Discrepancies > ₹100 auto-flagged; discrepancies > ₹10,000 trigger PagerDuty alert.

---

## 9. Resilience Plan

### 9.1 Idempotency Strategy

All state-mutating APIs require an `Idempotency-Key` header. Server-side deduplication:
```
Key:   idem:{service_name}:{idempotency_key}
Value: { status: 'completed', response: { ... } }
TTL:   24 hours

On request:
  Cache HIT (completed)    → return stored response immediately. No processing.
  Cache HIT (in-progress)  → return 202 with poll URL.
  Cache MISS               → write 'in-progress' immediately; write 'completed' + response on success.
```

| Endpoint | Idempotency Mechanism | Key Source |
| --- | --- | --- |
| POST /rides | Redis cache + DB UNIQUE on idempotency_key column | Client Idempotency-Key header |
| POST .../offer/accept | Optimistic lock on Driver.version + DB transition check | trip_id + driver_id composite |
| POST .../cancel | State check: if already CANCELLED return current state (no-op) | trip_id |
| PATCH /trips/.../status | State check: duplicate transition returns current state | trip_id + action |
| POST /payments | DB UNIQUE on idempotency_key + PSP idempotency key | SHA256(trip_id+':charge_1') |
| Kafka consumers | Redis SET of processed event_ids (TTL=24h); duplicates are no-ops | event_id from envelope |

### 9.2 Retry Policies

| Call Path | Max Attempts | Backoff | On Exhaustion |
| --- | --- | --- | --- |
| Ride Svc → Routing Svc | 3 | 100ms, 200ms, 400ms + 20% jitter | Straight-line dist × 1.25 fallback |
| Matching Svc → Redis (geo query) | 3 | Immediate, 50ms, 100ms | Fall back to DB geo query (degraded latency) |
| Trip Svc → CockroachDB | 3 | 50ms, 150ms, 500ms | Return 503; client retries with same Idempotency-Key |
| Payment Svc → PSP | 5 over 2h | 30s, 5m, 30m, 2h (async Kafka retries) | Move to payment.dlq; alert ops |
| Outbox Relay → Kafka | Infinite (bounded by lag alert) | Exp backoff up to 60s | Alert if lag > 1000 msgs for > 5 min |
| WS reconnect (client) | 5 | 1s, 2s, 4s, 8s, 30s | Fall back to HTTP long-poll (5s interval) |

### 9.3 Circuit Breakers

All outbound calls are wrapped in a circuit breaker (Resilience4j). Pattern: `CLOSED → OPEN → HALF-OPEN → CLOSED or OPEN`.

| Dependency | Open Threshold | Open Duration | Fallback Behavior |
| --- | --- | --- | --- |
| Routing Service (3rd party maps) | 5 failures / 10s | 30s | Straight-line distance × 1.25 road factor |
| Redis Surge Cache | 3 timeouts / 5s | 15s | Return 1.0x surge (no-surge safe default) |
| PSP Primary (Razorpay) | 3 failures / 30s | 60s | Route to secondary PSP (Stripe) |
| PSP Secondary (Stripe) | 3 failures / 30s | 60s | Queue in payment.pending; retry when circuit closes |
| Notification Svc (FCM/APNs) | 10 failures / 10s | 20s | Skip push; rider sees update on next app open/poll |
| CockroachDB write path | 2 timeouts / 5s | 30s | Return 503; preserve Idempotency-Key for client retry |

### 9.4 Failure Mode Analysis

| Failure Scenario | Impact | Detection | Mitigation & Recovery |
| --- | --- | --- | --- |
| Redis Geo Index shard crash | Drivers in that geo region invisible to matching | Prometheus: GEORADIUS returns 0 in active zone | Redis Sentinel promotes replica (<10s). Outbox Reader replays last 30s of location events to rebuild index. |
| Matching Service crash mid-offer | Driver got WebSocket offer; no DB record | Pod health check fails; K8s restart | `driver:lock` TTL expires (15s) → driver freed. New pod reads `trip:offer` state → continues from `current_index`. |
| CockroachDB primary node loss | Trip writes fail; ~30s window of errors | 503 rate spike in Grafana | CRDB Raft auto-promotes replica. Client idempotency keys prevent duplicates on retry. |
| PSP full outage | Payments fail for all COMPLETED trips | payment.result error rate > 5%; DLQ growing | Circuit breaker opens. All charges queued in Kafka. Reconciliation retries when PSP recovers. |
| Surge Calculator crash | Stale multipliers served until 60s TTL expires | No surge events for >60s | Safe default 1.0x served. K8s auto-restarts pod. New instance recalculates all cells within one 30s cycle. |
| Region-level failure | All services in region unavailable | Health checks fail across 2+ AZs for 60s | GeoDNS shifts new connections to nearest region within 60s. In-progress trips preserved in CRDB. Target RTO: <2 min. |

### 9.5 Backpressure

**Kafka Consumer Backpressure**
- Matching Service: if consumer lag on `ride.requested` > 5,000 messages → auto-scale pods via KEDA.
- Location update consumers: batch poll of 500 messages. If Redis write latency p99 > 50ms, drop GPS updates older than 5 seconds (stale; not useful for matching).

**API Gateway Rate Limiting**

| Endpoint / Client | Rate Limit | On Exceed |
| --- | --- | --- |
| GET /rides/estimate (per rider) | 10 req/min | 429 with Retry-After header |
| POST /rides (per rider) | 3 req/min | 429 — prevents duplicate ride spam |
| PUT /drivers/location (REST fallback) | 5 req/sec per driver | 200 OK but update silently dropped |
| Global ride requests | 1,200 req/sec (20% above 1k peak) | 503 with retry guidance |

---

## 10. Multi-Region Architecture

### 10.1 Region-Local Write Principle

Every hot-path write happens in the originating region. No cross-region calls on the critical path.

| Operation | Writes To | Cross-Region? | Why |
| --- | --- | --- | --- |
| Fare estimate | Local CockroachDB | No | Expires in 5 min |
| POST /rides (trip create) | Local CockroachDB | No | Rider's home region owns the trip |
| Driver GPS update | Local Redis + Kafka | No | Geo queries are always local |
| Trip state change | Local CockroachDB + Outbox | No | Trip lifecycle stays in originating region |
| Payment | Local CockroachDB | No | Processed in rider's region |
| Analytics replication | Cross-region async via Kafka | Yes (async, 5-30 min lag) | Not on hot path; acceptable lag |

### 10.2 CockroachDB Multi-Region Config

```sql
-- Survive loss of one full region:
ALTER DATABASE ridedb SURVIVE REGION FAILURE;

-- Trips: owned by the region where they were created (region-local fast writes)
ALTER TABLE trips   SET LOCALITY REGIONAL BY ROW;
ALTER TABLE riders  SET LOCALITY REGIONAL BY ROW;

-- Drivers: GLOBAL because a driver may be read from any region
-- Trade-off: ~150ms write latency for driver updates (acceptable — infrequent)
ALTER TABLE drivers SET LOCALITY GLOBAL;
```

### 10.3 Failover Strategy

| Failure Scope | Detection | Action | RTO |
| --- | --- | --- | --- |
| Single pod crash | K8s liveness probe | K8s restarts pod; traffic rerouted | < 30s |
| Redis primary failure | Redis Sentinel heartbeat miss | Sentinel promotes replica | < 10s |
| CockroachDB node failure | CRDB internal Raft | CRDB auto-promotes replica | < 30s |
| Kafka broker failure | Kafka controller detects loss | Partition leadership re-elected | < 60s |
| Full region failure | Health checks fail across 2+ AZs for 60s | GeoDNS shifts traffic | 60–120s |

### 10.4 GeoDNS Routing

- Riders and drivers connect to the API Gateway endpoint nearest to their GPS coordinates, resolved via AWS Route53 / Cloudflare GeoDNS.
- All requests are processed by local services. No request fan-out to other regions.
- A driver crossing a region boundary is migrated on their next app reconnect.

---

## 11. Compliance, Security & Admin/Ops

### 11.1 PCI DSS

- Raw card PANs are **NEVER** stored or logged in our systems. All card data is immediately tokenized by the PSP SDK before reaching our servers.
- PSP tokens in `payment_methods.psp_token` are encrypted at rest (AES-256) with keys in AWS KMS / GCP Cloud KMS.
- Payment APIs served over TLS 1.3. Network segmentation isolates Payment Service.
- PCI scope minimized: only the Payment Service pod and its direct DB access `payment_methods` data.

### 11.2 PII Encryption

| Data Field | Encryption | Key Management | Searchability |
| --- | --- | --- | --- |
| Phone numbers | AES-256-GCM at rest | AWS KMS CMK per region | SHA-256 hash index for exact lookup |
| Email addresses | AES-256-GCM at rest | AWS KMS CMK per region | SHA-256 hash index |
| Vehicle plate numbers | AES-256-GCM at rest | AWS KMS CMK | No index (read by driver_id only) |
| GPS location history | Encrypted at rest (Cassandra) | AWS KMS CMK | Partition key lookup only |
| PSP tokens | AES-256 + field-level encryption | Dedicated PCI KMS key | No plaintext access outside Payment Svc |

### 11.3 GDPR / DPDP Compliance

- **Right to Erasure**: DELETE endpoint anonymizes PII fields (`phone → ERASED_{uuid}`), sets `deleted_at`. Trip records retained but `rider_id` anonymized.
- **Data Minimization**: GPS location history TTL = 90 days (Cassandra TTL). Raw location beyond 90 days purged automatically.
- **Consent Tracking**: `gdpr_consent_ts` stored in `riders` table.
- **Data Portability**: Riders can request full trip history export via Admin API.

### 11.4 API Security Rules

- JWT tokens: 15-min access token, 30-day refresh token. `driver_id` / `rider_id` always from JWT claims — never from request body.
- All inter-service communication on mTLS (Istio service mesh).
- Driver/rider IDs are opaque UUIDs — no sequential IDs in public APIs (prevents enumeration attacks).
- Timestamps always server-generated. Client-supplied `client_ts` validated within ±30s.
- Fare amounts and surge multipliers always read from server-side DB/cache — never trusted from client.

### 11.5 Admin/Ops Feature Flags

| Flag / Tool | Default | Effect When Changed |
| --- | --- | --- |
| `surge_enabled:{region}` | true | false → all surge multipliers clamped to 1.0x immediately |
| `surge_max_cap:{region}` | 5.0 | Override max multiplier per region |
| `driver_offer_timeout_s` | 15 | Reduce offer window during high demand to speed matching |
| `payment_psp_primary:{region}` | razorpay | Switch primary PSP without deployment |
| `kill_switch:matching:{region}` | false | true → stop accepting new ride requests; existing trips continue |
| `kill_switch:payments` | false | true → halt payment processing; queue in Kafka |
| `location_update_interval_s` | 1 | Server instruction to clients on GPS frequency |

### 11.6 Observability

| Signal | Tool | Key Metrics |
| --- | --- | --- |
| Metrics | Prometheus + Grafana | Dispatch p50/p95/p99 latency, Kafka consumer lag, Redis hit rate, active WS connections, match success rate |
| Distributed Tracing | Jaeger / OpenTelemetry | Full trace per ride request; correlation_id in all Kafka events + HTTP headers |
| Log Aggregation | ELK Stack | Structured JSON. PII fields masked. Log level configurable via feature flag. |
| Alerting | PagerDuty | P1: dispatch p95 > 1s for 2 min; payment.dlq non-empty; region health check failure. P2: Kafka lag > 5000 for 5 min. |
| Synthetic Monitoring | Datadog Synthetics | End-to-end synthetic ride request every 60s per region. Alert if e2e > 3s. |
