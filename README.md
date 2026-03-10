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

<img width="1605" height="543" alt="Image" src="https://github.com/user-attachments/assets/fa721976-bfc7-4b9b-bfc3-521ff6a69b19" />
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

| Step | From → To | Protocol |
| :--- | :--- | :--- |
| 1. Rider requests ride | Rider App → API Gateway | HTTPS/REST |
| 2. Route to Ride Service | API Gateway → Ride Service | Internal HTTP/2 |
| 3. Fetch surge multiplier | Ride Service → Redis (Surge Cache) | Redis GET |
| 4. Write trip (SEARCHING) | Ride Service → CockroachDB + Outbox | SQL transaction |
| 5. Publish `ride.requested` | Outbox Reader → Kafka | Kafka produce |
| 6. Trigger matching | Ride Service → Matching Service | Internal async (Kafka) |
| 7. Geo query | Matching Service → Redis GeoSearch | GEORADIUS / ZRANGE |
| 8. Acquire driver lock | Matching Service → Redis (SETNX) | Redis atomic |
| 9. Push offer to driver | Matching Service → Connection Service → Driver App | WebSocket |
| 10. Driver accepts | Driver App → API Gateway → Trip Service | HTTPS/REST |
| 11. Update trip (MATCHED) | Trip Service → CockroachDB + Outbox | SQL transaction |
| 12. Notify rider | Notification Service ← Kafka | WebSocket push |

**SLO:** Steps 1–9 (dispatch decision) complete in ~100ms p50, well within the 1s p95 budget. Steps 1–12 (end-to-end request→acceptance) complete in ~130ms p50, within the 3s p95 budget. The 15-second driver response window is the dominant factor in p95 — we mitigate this by immediately trying the next candidate on timeout.

### 3.3 Driver Location Ingestion Data Flow

Drivers continuously send location updates while they are online.

1. The Driver App sends location updates over a persistent WebSocket connection: `{ driver_id, latitude, longitude, timestamp }`.
2. The Connection Service receives the update and forwards it to the Location Service through Kafka (`driver.location.updates` topic).
3. The Location Service updates the driver's latest location in the Redis Geo Index, which is used by the Matching Service to find nearby drivers quickly.
4. The same location events are asynchronously stored in Cassandra for historical analysis.
5. If the driver is currently serving a trip, the location update is forwarded through the Connection Service to the rider's app for real-time map tracking.

### 3.4 Scaling, Storage, and Trade-offs

* Transactional data such as trips and payments are stored in a distributed SQL database like CockroachDB or sharded PostgreSQL.
* Redis is used as an in-memory geo index for driver locations and dispatch queries, enabling low-latency driver lookup.
* Kafka acts as an event streaming backbone between services and allows the system to handle high-throughput location updates.
* The platform is deployed across multiple regions. Each region processes its own ride requests and driver updates to reduce latency.
---

## 4. Low-Level Design (LLD): Dispatch & Matching

The Dispatch/Matching component is the most latency-critical part of the ride-hailing system.
Its goal is to find the best available driver for a rider request within **≤1s p95 latency**, while ensuring a driver cannot receive multiple simultaneous ride offers.

The matching service performs spatial search, candidate ranking, driver reservation, and offer management.

### 4.1 Dispatch Flow

When a rider creates a ride request, the following steps occur:

1. **Ride Request Received**: The Ride Service publishes a `ride.requested` event. The Matching Service consumes the event and begins the dispatch process.
2. **Spatial Query**: The pickup location is mapped to a geo-cell. Nearby drivers are queried from the in-memory geo index.
3. **Candidate Filtering**: Drivers that are:
    * offline
    * currently in a trip
    * incompatible with the ride tier
    <br>...are removed from the candidate list.
4. **Driver Ranking**: Remaining drivers are ranked using heuristics such as:
    * distance to pickup
    * driver rating
    * acceptance rate
    * current direction of travel
5. **Driver Offer**: The top candidate driver receives a ride offer via WebSocket.
6. **Driver Response**: Driver must accept within a response window (≈10–15 seconds).
7. **Outcome Handling**:
    * Accept → Trip Service confirms the match.
    * Decline / Timeout → next candidate driver is offered the ride.
8. **Search Expansion**: If no suitable drivers are found, the search radius expands gradually until a match is found or the request fails.

### 4.2 Matching Algorithm

The matching algorithm balances speed, fairness, and driver utilization.

**Step 1 — Retrieve Nearby Drivers**
Matching Service queries the geo index to retrieve nearby drivers within an initial radius (e.g., 3 km).

**Step 2 — Filter Candidates**
Drivers are filtered based on:
* availability status
* ride tier compatibility
* recent activity (to avoid stale location data)

**Step 3 — Rank Candidates**
Drivers are ranked using a scoring function:
```
score = w1 * distance_to_pickup
      + w2 * driver_rating_penalty
      + w3 * acceptance_rate_penalty
```
Lower scores indicate better candidates.

**Step 4 — Offer Ride**
Drivers are offered rides sequentially based on ranking. If the driver does not respond within the offer window, the system moves to the next candidate.

**Step 5 — Expand Search**
If insufficient candidates are found, the search radius expands incrementally until a maximum limit is reached.

### 4.3 Concurrency Control & Double Dispatch Prevention

The system must ensure that a driver cannot be assigned to multiple riders simultaneously. Two mechanisms are used:

**Distributed Driver Reservation**
Before sending an offer, the Matching Service attempts to reserve the driver using a distributed lock. If the lock acquisition fails, the driver is already handling another request and is skipped.

**Database State Validation**
When a driver accepts the offer:
1. The Trip Service performs an atomic state transition.
2. The driver is marked as assigned to the trip.
3. If another request already claimed the driver, the operation fails and the matching process continues.

This dual approach ensures consistency while keeping dispatch latency low.

### 4.4 Offer Lifecycle State Machine

Each ride request progresses through an offer state machine during dispatch.

| State | Description |
| --- | --- |
| **SEARCHING** | Ride request created and matching begins |
| **OFFERING** | Ride offer sent to a candidate driver |
| **MATCHED** | Driver accepted the ride |
| **EXPANDING** | Search radius expanded due to insufficient drivers |
| **FAILED** | No driver accepted within timeout window |

**State Transitions**
```
SEARCHING → OFFERING → MATCHED
                ↓
           DECLINED/TIMEOUT
                ↓
            OFFER NEXT DRIVER
                ↓
            EXPANDING SEARCH
```
If the system fails to match a driver within an overall timeout window (e.g., 60 seconds), the rider is notified that no drivers are available.

### 4.5 Handling Stale Driver Locations

Drivers periodically send location updates while online. If the system stops receiving updates from a driver:
* the driver is considered stale
* the driver is excluded from matching queries
* the driver's status is eventually updated to offline

This prevents dispatching rides to drivers who have lost connectivity.

### 4.6 Matching Service Crash Recovery

The Matching Service is stateless and processes ride requests from the event stream. If a Matching Service instance fails:
* Another instance in the consumer group takes over the event partition.
* The matching state is reconstructed using cached dispatch state and trip status.
* Driver reservations automatically expire after the offer window.

This ensures dispatch continues without manual intervention.

### 4.7 Dispatch Service Internal Components

The Matching Service is composed of several internal components responsible for different parts of the dispatch process.

**MatchingService**
Coordinates the overall matching process. Implements logic to orchestrate driver search, rank candidates, send ride offers, and handle responses.
```java
class MatchingService {
    GeoIndex geoIndex;
    RankingService rankingService;
    DispatchLockManager lockManager;
    OfferService offerService;

    matchRide(RideRequest request);
}
```

**GeoIndex**
Handles spatial driver lookup. Backed by an in-memory geo index (e.g., Redis).
```java
class GeoIndex {
    List<DriverLocation> findNearbyDrivers(Location pickup, int radius);
}
```

**RankingService**
Ranks candidate drivers. Ranking signals include proximity, driver rating, and acceptance history.
```java
class RankingService {
    List<Driver> rankDrivers(List<Driver> candidates, RideRequest request);
}
```

**DispatchLockManager**
Ensures drivers cannot receive multiple simultaneous offers. Implemented using a distributed lock in the caching layer.
```java
class DispatchLockManager {
    boolean tryLockDriver(UUID driverId);
    void releaseDriver(UUID driverId);
}
```

**OfferService**
Handles communication with drivers. Offers are delivered via the Connection Service using WebSocket connections.
```java
class OfferService {
    void sendRideOffer(UUID driverId, RideRequest request);
    void handleAccept(UUID driverId, UUID tripId);
    void handleDecline(UUID driverId, UUID tripId);
}
```

---


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
