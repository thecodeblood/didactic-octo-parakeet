# Multi-Region Ride-Hailing Platform

## Introduction
A ride-hailing platform connects riders who need transportation with nearby drivers. The system must support real-time location updates, low-latency driver matching, dynamic pricing, trip lifecycle management, and payment processing.

This system must operate across multiple regions, support millions of drivers and riders, and meet strict latency requirements:

- Dispatch decision ≤ **1s p95**
- End-to-end request to acceptance ≤ **3s p95**
- Availability ≥ **99.95%**

The architecture uses **microservices, event streaming, geo-indexing, and distributed databases** to achieve scalability and reliability.

---

# Requirements

## 1. Functional Requirements

- Ingest real-time driver location updates (**1–2 updates/sec per driver**)
- Allow riders to request rides with pickup, destination, tier, and payment method
- Match riders with nearby drivers within **≤1s dispatch latency**
- Support **dynamic surge pricing** based on supply–demand per geo-cell
- Manage trip lifecycle: **request → assign → start → pause → end**
- Calculate fares and generate receipts
- Process payments via external PSPs with retries
- Send **push/SMS notifications** for ride events
- Provide **admin tools** for feature flags and monitoring

---

## 2. Non-Functional Requirements

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

# Customer User Journey

## Rider Journey

1. **Open App**  
   Rider enters pickup and destination.

2. **Fare Estimate**  
   System calculates route and shows estimated fare and ETA.

3. **Request Ride**  
   Rider selects tier and payment method.

4. **Driver Matching**  
   System finds nearby drivers and assigns one.

5. **Track Driver**  
   Rider tracks driver location in real time.

6. **Trip Start**  
   Driver starts the trip after pickup.

7. **Trip End**  
   Driver ends trip at destination.

8. **Payment & Receipt**  
   System processes payment and sends receipt.

---

## Driver Journey

1. **Go Online**  
   Driver opens app and sets status to online.

2. **Location Updates**  
   Driver app sends continuous GPS updates.

3. **Receive Ride Request**  
   Nearby ride request appears.

4. **Accept Ride**  
   Driver navigates to pickup location.

5. **Start Trip**  
   Driver picks up rider and starts trip.

6. **Complete Trip**  
   Driver ends trip at destination.

7. **Payment Settlement**  
   Platform processes payment and updates earnings.

---

# Defining Core Entities

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
<img width="1230" height="739" alt="brave_screenshot_excalidraw com" src="https://github.com/user-attachments/assets/f18b3e85-b0fa-4c88-a3c1-1f7bee3bd9ca" />
---

# Scale Analysis & Capacity Planning

Before designing the architecture, we derive precise throughput numbers.

## Given Constraints

| Metric | Value | Notes |
|------|------|------|
| Concurrent drivers | 300,000 | Online and sending location updates |
| Location update rate | 1–2/sec | Avg ~1/sec |
| Location write throughput | 300k–600k/sec | Peak ~500k/sec |
| Ride requests | 60k/min (~1000/sec) | Metro concentration |
| Dispatch SLO | ≤1 sec p95 | Matching latency |
| End-to-end SLO | ≤3 sec p95 | Includes driver response |
| Availability | 99.95% | ~4.4 hours downtime/year |
| Regions | 3 | Active-active |

Regions example:

- ap-south-1
- ap-southeast-1
- eu-west-1

---

# Derived System Sizing

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

# API / System Interface

## Rider APIs

### Create Ride Request
