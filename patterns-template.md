# Patterns-in-Code Document – HELEP

---

## Part A – Pre-implemented Patterns

### A.1 Choreographed Saga

The saga is the backbone of the entire SOS flow. Instead of one service
orchestrating everything from the center, each service knows its role and
reacts to what it receives.

When a citizen sends an SOS, sos-service creates the incident and publishes
a sos.triggered event to Kafka. dispatch-service is listening on that topic.
It finds a free responder, reserves them in the database, and publishes
responder.assigned. notification-service picks that up and sends the alerts,
then publishes notification.sent. analytics-service consumes every single one
of these events and updates the statistics counters.

The compensation step is in dispatch-service/app/main.py in the handle_cancel
function. When a sos.cancelled event arrives, the service calls
release_assignment to free the responder and publishes a responder.confirmed
event with status RELEASED. This rolls back the reservation that was made
during the forward flow.

The rollback trigger is the sos.cancelled event published by sos-service when
a citizen cancels their alert.

### A.2 Pub/Sub via Apache Kafka

Every service has an app/events.py file that handles all Kafka communication
using the aiokafka library. Services never call each other directly. They only
publish to topics and consume from topics.

The consumer is configured with enable_auto_commit set to false. This is
important. The offset is only committed after the handler function finishes
successfully. If the service crashes in the middle of processing an event,
Kafka will redeliver it when the service restarts. This gives us at-least-once
delivery which is exactly what we need for an emergency platform where losing
an SOS is not acceptable.

The partition key matters here too. Every publish call passes incident_id as
the key. Kafka routes all messages with the same key to the same partition.
This guarantees that events for the same incident are always processed in the
order they were published. Without this, a cancellation could arrive before
the original trigger and leave the system in a broken state.

### A.3 Repository

Every service has an app/db.py file that contains all the database logic.
Route handlers and event consumers never write SQL directly. They call
functions like all_free_responders, reserve_responder_for, or assignment_for
and get back clean Python objects.

If we had let route handlers query SQLite directly, every endpoint would be
coupled to the database schema. Changing a table structure would mean hunting
through all the route files to update queries. With the repository pattern,
the change stays inside db.py and nothing else breaks.

### A.4 Strategy

The responder matching logic in dispatch-service/app/matching.py is built
around a strategy interface. The matcher function reads the MATCHER environment
variable and returns the right implementation. Right now there are two: simple
which picks the first available responder, and nearest which uses the
haversine formula to calculate distances and picks the closest one.

Switching between them requires no code change at all. You update the
environment variable in the Helm values and redeploy. The rest of the service
does not know or care which strategy is active.

A third strategy could be a round-robin matcher that cycles through available
responders evenly to distribute workload. It would implement the same pick
method and register itself under the name roundrobin in the matcher function.

### A.5 Outbox-lite

In sos-service/app/main.py the trigger function creates the SOS record in
SQLite and then immediately publishes the sos.triggered event to Kafka within
the same async function call. This is the outbox-lite pattern — the write and
the publish are treated as one logical operation.

It is called lite because a proper Outbox pattern would write the event to a
database table first and have a separate process relay it to Kafka. This
guarantees the event is published even if Kafka is temporarily unavailable at
the moment of the write. Our implementation skips that relay step which means
if Kafka is down at the exact moment of the SOS, the event could be lost.
The trade-off was acceptable for a prototype.

### A.6 Circuit Breaker

The circuit breaker is implemented in dispatch-service/app/circuit_breaker.py
and integrated into dispatch-service/app/main.py.

There are three circuit breakers, one for each critical operation in the
dispatch flow: fetching free responders from the database, running the matcher
to pick one, and reserving the chosen responder. Each one is created with a
failure threshold of 3 and a recovery timeout of 30 seconds.

The state machine works like this. The circuit starts CLOSED which means
requests flow through normally. Every time the wrapped function throws an
exception, the failure counter increments. When it hits 3, the circuit moves
to OPEN. In the OPEN state every call is rejected immediately without even
trying the function. After 30 seconds the circuit moves to HALF_OPEN and
allows one test call through. If that call succeeds the circuit resets to
CLOSED. If it fails the circuit goes back to OPEN and the timeout starts again.

This prevents a database failure from turning into an infinite loop of retries
that could create duplicate assignments or exhaust resources.

The status of all three circuit breakers is visible at runtime through the
GET /circuit-breaker/status endpoint added to dispatch-service.

---

## Part B – Patterns We Added

### B.1 Health Endpoint Pattern

Every service exposes two endpoints: /healthz for liveness and /readyz for
readiness. These are defined in each service's app/main.py.

The liveness endpoint just returns a 200 with status ok. Kubernetes uses this
to know whether the process is alive. If it stops responding, the pod gets
restarted.

The readiness endpoint does an actual check. In services that depend on Kafka
it calls the health function from events.py which verifies the Kafka connection
is working. If Kafka is unreachable it returns 503 and Kubernetes stops sending
traffic to that pod until it recovers.

The alternative would have been a single health endpoint for both checks. The
problem with that is a service might be alive but not ready — for example if
it just started and the Kafka consumer is still connecting. Splitting the two
concerns means Kubernetes can make smarter decisions about when to restart
versus when to just stop routing traffic.

### B.2 Sidecar Pattern via Prometheus

Every service mounts a /metrics endpoint using the prometheus_client library.
This is done in app/main.py with a single line that mounts the Prometheus ASGI
app at that path. The service itself does not push metrics anywhere. Prometheus
scrapes the endpoint on its own schedule.

Custom counters are defined at the top of each main.py. For example
helep_sos_triggered_total in sos-service and helep_dispatch_assigned_total in
dispatch-service. Every time the relevant event is processed the counter
increments.

The alternative would have been to push metrics from each service to a central
collector. The scrape model is better here because the service does not need to
know where Prometheus is or whether it is even running. The monitoring
infrastructure is completely decoupled from the application.

---

## Part C – Anti-pattern Avoided

The shared database anti-pattern is explicitly avoided throughout this project.
In a distributed system it is tempting to have all services connect to one
central database because it makes queries simple and avoids the need to
synchronise data through events. The problem is it creates tight coupling
between services at the data layer. One service changing a table schema can
break every other service. Deployments have to be coordinated. Independent
scaling becomes impossible.

In HELEP every service has its own SQLite database mounted on its own PVC.
dispatch-service/app/db.py only contains tables and queries for responder
state. It has no knowledge of users, SOS records, or notifications. If any
data needs to cross a service boundary it does so through a Kafka event, not
a direct database read. This is visible in the complete absence of any shared
database connection string across the five services.