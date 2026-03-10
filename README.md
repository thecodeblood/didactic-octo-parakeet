# Multi-Region Ride-Hailing Platform - System Design

## Introduction
A ride-hailing platform connects riders who need transportation with nearby drivers. The system must support real-time location updates, low-latency driver matching, dynamic pricing, trip lifecycle management, and payment processing.

This system must operate across multiple regions, support millions of drivers and riders, and meet strict latency requirements:

- Dispatch decision ≤ **1s p95**
- End-to-end request to acceptance ≤ **3s p95**
- Availability ≥ **99.95%**

The architecture uses **microservices, event streaming, geo-indexing, and distributed databases** to achieve scalability and reliability.

---

## Requirements

### 1. Functional Requirements
- Ingest real-time driver location updates (**1–2 updates/sec per driver**)
- Allow riders to request rides with pickup, destination, tier, and payment method
- Match riders with nearby drivers within **≤1s dispatch latency**
- Support **dynamic surge pricing** based on supply–demand per geo-cell
- Manage trip lifecycle: **request → assign → start → pause → end**
- Calculate fares and generate receipts
- Process payments via external PSPs with retries
- Send **push/SMS notifications** for ride events
- Provide **admin tools** for feature flags and monitoring

### 2. Non-Functional Requirements
- Dispatch latency **p95 ≤ 1s**
- Request → acceptance **p95 ≤ 3s**
- **99.95% availability** for dispatch APIs
- Support **300k concurrent drivers**
- Handle **60k ride requests/min**
- Process **~500k location updates/sec globally**
- Multi-region deployment with **region-local writes**
- APIs must be **idempotent** for unreliable mobile networks
- Protect **PII and payments (PCI, GDPR/DPDP compliance)**

---

## Customer User Journey

### Rider Journey
1. **Open App** - Rider enters pickup and destination.
2. **Fare Estimate** - System calculates route and shows estimated fare and ETA.
3. **Request Ride** - Rider selects tier and payment method.
4. **Driver Matching** - System finds nearby drivers and assigns one.
5. **Track Driver** - Rider tracks driver location in real time.
6. **Trip Start** - Driver starts the trip after pickup.
7. **Trip End** - Driver ends trip at destination.
8. **Payment & Receipt** - System processes payment and sends receipt.

### Driver Journey
1. **Go Online** - Driver opens app and sets status to online.
2. **Location Updates** - Driver app sends continuous GPS updates.
3. **Receive Ride Request** - Nearby ride request appears.
4. **Accept Ride** - Driver navigates to pickup location.
5. **Start Trip** - Driver picks up rider and starts trip.
6. **Complete Trip** - Driver ends trip at destination.
7. **Payment Settlement** - Platform processes payment and updates earnings.

---

## Defining Core Entities
- **Rider** – User requesting rides
- **Driver** – Driver providing rides
- **Vehicle** – Driver vehicle information
- **RideRequest** – Initial rider request
- **Trip** – Active or completed ride lifecycle
- **Location** – Driver GPS updates
- **SurgePricing** – Pricing multiplier per geo-cell
- **Payment** – Trip payment transaction
- **Notification** – Ride event messages
- **GeoCell / Region** – Geographic partition

---

## Scale Analysis & Capacity Planning
Before designing the architecture, we derive precise throughput numbers.

### Given Constraints
| Metric | Value | Notes |
|------|------|------|
| Concurrent drivers | 300,000 | Online and sending location updates |
| Location update rate | 1–2/sec | Avg ~1/sec |
| Location write throughput | 300k–600k/sec | Peak ~500k/sec |
| Ride requests | 60k/min (~1000/sec) | Metro concentration |
| Dispatch SLO | ≤1 sec p95 | Matching latency |
| End-to-end SLO | ≤3 sec p95 | Includes driver response |
| Availability | 99.95% | ~4.4 hours downtime/year |
| Regions | 3 | Active-active (ap-south-1, ap-southeast-1, eu-west-1) |

### Derived System Sizing
| Component | Rationale | Capacity |
|----------|----------|---------|
| Kafka driver.location.updates | 500k msg/s | 512 partitions |
| Redis Geo Index | 300k driver keys | ~60MB hot data |
| WebSocket Connections | 600k connections | 20 pods |
| Matching Service | 1000 req/s | 10 pods |
| Trip DB (CockroachDB) | 20M trips/day | 3-node cluster |
| Location History (Cassandra) | 500k writes/sec | 6-node cluster |
| Surge Calculator | ~800 geo cells | 2 replicas |

---

## 1. APIs & System Interfaces

### 1.1 External APIs (via API Gateway)

These APIs are consumed by Rider App and Driver App.

#### Rider APIs

**Request Fare Estimate**
Calculates ETA and estimated fare.
*   **Endpoint:** `GET /rides/estimate`
*   **Query:** `pickup_lat`, `pickup_lon`, `dest_lat`, `dest_lon`, `ride_tier`
*   **Handled by:** Ride Service, Routing Service, Surge Calculator
*   **Response:**
    ```json
    {
      "estimated_fare": 240,
      "eta_minutes": 5,
      "surge_multiplier": 1.2
    }
    ```

**Create Ride Request**
Creates a new ride and triggers matching.
*   **Endpoint:** `POST /rides`
*   **Request:**
    ```json
    {
      "rider_id": "uuid",
      "pickup_location": { "lat": 12.9, "lon": 77.6 },
      "destination": { "lat": 12.8, "lon": 77.5 },
      "ride_tier": "standard",
      "payment_method": "card"
    }
    ```
*   **Handled by:** Ride Service, Matching Service, Surge Calculator
*   **Response:**
    ```json
    {
      "ride_id": "uuid",
      "status": "SEARCHING"
    }
    ```

**Get Ride Status**
*   **Endpoint:** `GET /rides/{ride_id}`
*   **Handled by:** Trip Service
*   **Response:**
    ```json
    {
      "ride_id": "uuid",
      "status": "DRIVER_ASSIGNED",
      "driver_id": "uuid",
      "eta": 3
    }
    ```

**Cancel Ride**
*   **Endpoint:** `POST /rides/{ride_id}/cancel`
*   **Handled by:** Trip Service, Matching Service

#### Driver APIs

**Update Driver Location**
*   **Endpoint:** `POST /drivers/location`
*   **Handled by:** Location Service
*   **Request:**
    ```json
    {
      "driver_id": "uuid",
      "latitude": 12.9,
      "longitude": 77.6,
      "timestamp": "2026-03-10T12:00:00Z"
    }
    ```
*   **Hot path:** Redis Geo Index
*   **Cold path:** Kafka → Cassandra

**Driver Online / Offline**
*   **Endpoint:** `POST /drivers/status`
*   **Handled by:** Location Service, Matching Service
*   **Request:**
    ```json
    {
      "driver_id": "uuid",
      "status": "ONLINE"
    }
    ```

**Accept Ride**
*   **Endpoint:** `POST /drivers/rides/{ride_id}/accept`
*   **Handled by:** Matching Service, Trip Service

**Reject Ride**
*   **Endpoint:** `POST /drivers/rides/{ride_id}/reject`
*   **Handled by:** Matching Service

#### Trip APIs

**Start Trip**
*   **Endpoint:** `POST /trips/{trip_id}/start`
*   **Handled by:** Trip Service

**Pause Trip**
*   **Endpoint:** `POST /trips/{trip_id}/pause`

**End Trip**
*   **Endpoint:** `POST /trips/{trip_id}/end`
*   **Handled by:** Trip Service, Payment Service
*   **Response:**
    ```json
    {
      "trip_id": "uuid",
      "fare": 320,
      "distance_km": 8.2,
      "duration_minutes": 15
    }
    ```

#### Payment APIs

**Process Payment**
*   **Endpoint:** `POST /payments`
*   **Handled by:** Payment Service
*   **Request:**
    ```json
    {
      "trip_id": "uuid",
      "amount": 320,
      "payment_method": "card"
    }
    ```

**Payment Status**
*   **Endpoint:** `GET /payments/{payment_id}`

#### Admin APIs

**Feature Flags**
*   **Endpoint:** `POST /admin/feature-flags`
*   **Handled by:** Admin Service

**Kill Switch**
*   **Endpoint:** `POST /admin/services/{service}/disable`

### 1.2 Internal System Interfaces

These are service-to-service interfaces inside the system.

#### Ride Service
*   `estimateFare(pickup, destination, tier)`
*   `createRideRequest(riderId, pickup, destination)`
*   `applySurgeMultiplier(cellId)`
*   **Calls:** Routing Service, Surge Calculator, Trip Service, Matching Service

#### Trip Service
*   `createTrip(rideRequest)`
*   `updateTripState(tripId, state)`
*   `startTrip(tripId)`
*   `endTrip(tripId)`
*   `calculateFare(tripId)`
*   `publishTripEvent(tripId, state)`
*   **Uses:** CockroachDB, Kafka Outbox

#### Matching Service
*   `findNearbyDrivers(location, radius)`
*   `rankDriverCandidates(driverList)`
*   `sendRideOffer(driverId, rideId)`
*   `handleDriverResponse(accept/decline)`
*   `reassignDriver(rideId)`
*   **Uses:** Redis Geo, Redis locks, Connection Service

#### Location Service
*   `ingestDriverLocation(driverId, lat, lon)`
*   `updateGeoIndex(driverId, location)`
*   `publishLocationEvent()`
*   **Writes:** Redis, Kafka

#### Connection Service
*   `registerConnection(userId, socketId)`
*   `pushMessage(userId, message)`
*   `broadcastEvent(event)`
*   **Uses:** Redis connection registry

#### Surge Calculator
*   `computeSurgeMultiplier(cellId)`
*   `getSurgeMultiplier(cellId)`
*   `publishSurgeUpdate()`
*   **Reads:** Redis supply, Kafka demand

#### Payment Service
*   `initiatePayment(tripId)`
*   `callPSP(paymentPayload)`
*   `retryPayment(paymentId)`
*   `reconcilePayments()`
*   **Uses:** CockroachDB, PSP APIs

#### Notification Service
*   `sendPush(userId, message)`
*   `sendSMS(userId, message)`
*   `consumeTripEvents()`
*   **Consumes:** Kafka `trip.state.changed`

#### Routing Service
*   `calculateRoute(pickup, destination)`
*   `estimateETA(route)`
*   `estimateDistance(route)`
*   **Uses:** Road graph, Map provider APIs

---

## 2. Data Model (ERD for Dispatch / Trip Service)

Using CockroachDB / Postgres, partitioned by `region`.

**Table: Trips**
*   `id` (UUID, Primary Key)
*   `region` (Enum/String, Partition Key)
*   `rider_id` (UUID, Indexed)
*   `driver_id` (UUID, Nullable, Indexed)
*   `status` (Enum: PENDING, ACCEPTED, EN_ROUTE, IN_PROGRESS, COMPLETED, CANCELLED)
*   `pickup_lat`, `pickup_lon`, `dropoff_lat`, `dropoff_lon` (Float)
*   `tier` (String)
*   `fare_estimated` (Decimal)
*   `created_at`, `updated_at` (Timestamp)

**Table: Drivers (State & Metadata)**
*Typically, raw location sits in Redis, but persistent state sits in DB.*
*   `id` (UUID, Primary Key)
*   `region` (String, Partition Key)
*   `tier` (Enum)
*   `status` (Enum: OFFLINE, ONLINE, IN_TRIP)

**Redis Key-Value Design for Dispatch**
*   `driver_location:{region}:{geohash}` -> ZSET (GeoHash) of Driver IDs
*   `driver_session:{driver_id}` -> Hashmap: `{status: "AVAILABLE", lat: 37.77, lon: -122.41}`
*   `dispatch_lock:{driver_id}` -> TTL 10s (prevents double dispatch)

<img width="1230" height="739" alt="Image" src="https://github.com/user-attachments/assets/2614b6fd-7377-470c-a1b5-b09523cf41d4" />
---

## 3. High-Level Design (HLD)

<img width="1774" height="798" alt="Image" src="https://github.com/user-attachments/assets/a4cdcc36-2d85-4efc-9637-d574b5fc26ef" />

### 3.1 Architecture Components

The ride-hailing platform follows a **microservices architecture with event-driven communication**.  
Services communicate synchronously via APIs and asynchronously via **Kafka event streams** to support high throughput and low latency.

The platform is deployed in **multiple active-active regions**, where each region processes local ride requests, driver updates, and dispatch decisions to minimize latency and avoid cross-region calls on the hot path.

The architecture is organized into the following layers.

#### API Layer

**API Gateway**
The API Gateway is the entry point for Rider and Driver mobile applications.

Responsibilities:
- TLS termination
- Authentication (JWT)
- Rate limiting
- Request routing
- WebSocket upgrade handling

Clients communicate with the gateway using **HTTPS REST APIs and WebSockets**.

#### Core Services

**Ride Service**
Handles ride request creation and fare estimation.

Responsibilities:
- Create ride requests
- Estimate fares
- Apply surge multipliers

**Trip Service**
Manages the **trip lifecycle state machine**.

Responsibilities:
- Track trip states (SEARCHING → ASSIGNED → IN_PROGRESS → COMPLETED)
- Persist trip records
- Publish trip events using the **Transactional Outbox pattern**

**Matching Service**
The core dispatch engine responsible for pairing riders with drivers.

Responsibilities:
- Perform geo queries for nearby drivers
- Rank driver candidates
- Send ride offers
- Handle accept/decline/timeout events

It relies on **Redis Geo Index and distributed locks** to ensure fast and consistent driver assignment.

**Location Service**
Handles high-frequency driver GPS updates.

Data Flow:
- **Hot Path:** Driver location stored in Redis Geo Index for fast dispatch queries
- **Cold Path:** Location updates streamed through Kafka and stored in Cassandra for historical analysis

**Connection Service**
Maintains persistent **WebSocket connections** with Rider and Driver applications.

Responsibilities:
- Manage connection registry
- Deliver real-time ride offers and trip updates
- Push driver location updates to riders

#### Support Services

**Surge Calculator**
Computes surge pricing multipliers based on **supply–demand imbalance per H3 geo-cell**.

Responsibilities:
- Track active drivers and ride demand
- Compute surge multipliers
- Cache surge values in Redis

**Payment Service**
Handles payment orchestration with external **Payment Service Providers (PSPs)**.

Responsibilities:
- Initiate payments
- Retry failed transactions
- Perform reconciliation

**Notification Service**
Sends ride-related notifications to riders and drivers.

Supported channels:
- Push notifications (FCM / APNs)
- SMS fallback

**Routing Service**
Computes routes, ETA, and trip distance.

Responsibilities:
- Route computation
- Distance estimation
- ETA calculation

Uses **road network graphs and pathfinding algorithms such as A\***.

**Admin Service**
Provides operational tooling for platform administrators.

Responsibilities:
- Feature flags
- Kill switches
- Region-specific configuration

#### Data Layer

**CockroachDB**
Primary transactional database storing:
- Trips
- Payments
- Rider data

Provides **strong consistency and multi-region replication**.

**Cassandra**
Used for high-throughput storage of **driver location history**.
- Optimized for time-series writes
- Data retention via TTL (90 days)

**Redis Geo Index**
An in-memory datastore used for **real-time spatial queries**.

Stores:
- Driver coordinates
- Dispatch locks
- Surge multipliers
- WebSocket connection registry

Provides **sub-millisecond lookup latency**.

**Kafka (Event Bus)**
The central event streaming backbone connecting microservices.

Key event streams include:
- `driver.location.updates`
- `ride.request.created`
- `driver.assigned`
- `trip.state.changed`
- `payment.processed`

Kafka enables **high-throughput, fault-tolerant, and decoupled communication between services**.

### 3.2 Request Data Flow — Ride Request to Match
This is the critical hot path. Every step must complete within the p95 budget.

| Step | From → To | Protocol | Latency Budget |
| :--- | :--- | :--- | :--- |
| 1. Rider requests ride | Rider App → API Gateway | HTTPS/REST | ~20ms (TLS + auth) |
| 2. Route to Ride Service | API Gateway → Ride Service | Internal HTTP/2 | ~5ms |
| 3. Fetch surge multiplier | Ride Service → Redis (Surge Cache) | Redis GET | ~2ms |
| 4. Write trip (SEARCHING) | Ride Service → CockroachDB + Outbox | SQL transaction | ~15ms |
| 5. Publish `ride.requested` | Outbox Reader → Kafka | Kafka produce | ~5ms async |
| 6. Trigger matching | Ride Service → Matching Service | Internal async (Kafka) | ~10ms |
| 7. Geo query | Matching Service → Redis GeoSearch | GEORADIUS / ZRANGE | ~3ms |
| 8. Acquire driver lock | Matching Service → Redis (SETNX) | Redis atomic | ~1ms |
| 9. Push offer to driver | Matching Service → Connection Service → Driver App | WebSocket | ~15ms |
| 10. Driver accepts | Driver App → API Gateway → Trip Service | HTTPS/REST | ~30ms |
| 11. Update trip (MATCHED) | Trip Service → CockroachDB + Outbox | SQL transaction | ~15ms |
| 12. Notify rider | Notification Service ← Kafka | WebSocket push | ~20ms async |

**SLO:** Steps 1–9 (dispatch decision) complete in ~100ms p50, well within the 1s p95 budget. Steps 1–12 (end-to-end request→acceptance) complete in ~130ms p50, within the 3s p95 budget. The 15-second driver response window is the dominant factor in p95 — we mitigate this by immediately trying the next candidate on timeout.

### 3.3 Driver Location Ingestion Data Flow
This is the highest-throughput path in the system: 500k writes/second globally.

1. **Driver App** sends GPS frame over persistent WebSocket every 1–2 seconds: `{ driver_id, lat, lng, heading, speed, client_ts }`.
2. **Connection Service** receives the frame, extracts `driver_id` from the authenticated WebSocket session, and forwards to **Location Service** via Kafka (`driver.location.updates` topic).
3. **Location Service hot path**: Compute geohash; update Redis Geo Index (`GEOADD`); if driver status changed cell, remove from old cell. Apply latest-timestamp-wins — drop if `client_ts` < stored `ts`.
4. **Location Service cold path**: Forward the raw event to Kafka. A separate Location History Consumer batches 500 events and bulk-writes to Cassandra with `driver_id` + `date` as partition key.
5. **Location Service trip relay**: If driver is on an active trip (checked via Redis `driver:status:{id}`), look up rider's Connection Service instance in the Connection Registry and push the location update over the rider's WebSocket for live map tracking.

### 3.4 Scaling, Storage, and Trade-offs
*   **Transactions (Trip & Payment)**: Use **CockroachDB (or Sharded Postgres)**. CockroachDB natively supports multi-region deployments with geo-partitioning. By partitioning trip tables by `region_id`, writes are entirely localized to the region without cross-region consensus, keeping latencies low. 
*   **Location Ingestion & Fast Queries**: Use **Redis** for driver state and location (`GEOADD`, `GEORADIUS`). With 500k updates/sec, Redis must be clustered and partitioned by region or geohash. WebSockets or gRPC streams handle the ingestion.
*   **Events**: Use **Kafka**. Partitioned by region and decoupled. It buffers location updates for stream processors (e.g., Flink) to compute aggregate supply per geohash for the Surge Pricing Service.
*   **Multi-Region Strategy**: Active-Active per region. A ride request in US-East stays in US-East. There is no cross-region sync on the hot path. Async replication can sync aggregates to a global analytical platform.


---

## 4. Low-Level Design (LLD): Dispatch & Matching

The Dispatch flow is critical because it must assign drivers within `p95 < 1s`. 

### 4.1 Matching Algorithm
1.  **Spatial Query**: Dispatch Service receives `pickup_lat`, `pickup_lon`. It computes the Geohash (e.g., precision 6) and queries the Redis Geo-Index for drivers within a 3km to 5km radius.
2.  **Filter**: Exclude drivers whose state is not `AVAILABLE` or who don't match the requested tier (e.g., UberX vs Premium).
3.  **Rank**: Use an in-memory ranking heuristic or a fast ML model (shadow inferred) that ranks candidates based on:
    *   Straight-line distance (proxy for ETA).
    *   Driver acceptance rate and preferences.
    *   Direction of travel (to avoid U-turns).
4.  **Offer Generation**: 
    *   Perform an atomic check-and-set (CAS) or use a distributed lock in Redis to set the driver's status to `RESERVED` for `N` seconds (e.g., 10s).
    *   If successful, send an offering dispatch event.

### 4.2 Concurrency & Reassignment
*   **Locking**: To avoid assigning one driver to two riders simultaneously, Redis uses a Lua script or `SETNX` logic to atomically hold a lock.
*   **Timeouts**: If the driver fails to respond in 10s, a Redis Key Expiry event or a Kafka delayed queue triggers a re-dispatch. The Dispatch Service fetches the next driver from the ranked list and repeats.

---

## 5. Resilience, Fault Tolerance, & Flaky Networks

### 5.1 Flaky Mobile Networks & Idempotency
*   Mobile networks frequently drop connections. All mutating API requests (e.g., Request Ride, Accept Dispatch) require an `Idempotency-Key` (UUID) generated by the mobile client.
*   The API Gateway or Service uses Redis (or a dedicated idempotency Postgres table) to cache responses for `Idempotency-Key`. If the client retries a `POST /v1/rides` with the same key, it immediately returns the cached `ride_id` rather than dispatching a duplicate trip.

### 5.2 Retry Strategies & External PSPs
*   Payments through external PSPs are slow and out of our control. 
*   **Async Capture**: The initial ride request only performs an API authorization (or a risk check). The actual charge happens asynchronously via a chronologically ordered message queue (Kafka or AWS SQS) after the trip finishes.
*   **Exponential Backoff**: For PSP failures (e.g., 503, rate limits), the Payments Orchestrator retries the charge using exponential backoff with jitter to prevent thundering herds on PSP recovery.

### 5.3 Circuit Breakers
*   Services heavily utilize **Circuit Breakers** (e.g., Resilience4j) for external RPCs (like the Notifications provider or PSPs).
*   If the Push Notification provider is suffering an outage, the Circuit Breaker opens, fast-failing requests to prevent thread starvation on our Dispatch workers. Fallbacks (like SMS routing if Push fails) can be engaged automatically.

### 5.4 Backpressure
*   Driver location ingestion (500k/s) is susceptible to load spikes. If downstream services (like Postgres or Flink) degrade, the ingress gateway drops location updates (load shedding) rather than crashing. Location pings are loss-tolerant events—missing a 1-second ping is acceptable since the next one will overlap it. Using Kafka intrinsically provides a buffer to absorb backpressure.
