# Rally API Gateway

Single entry point for the RallyDeals platform. Routes client traffic to the internal
services, validates JWTs, injects identity headers downstream, adds a tracing header,
and rate-limits every request with Redis. No business logic, no database.

**Spec:** `groupdeal-architecture.md` §4.1, `PROJECT-REFERENCE.md` §3 (identity model),
`gaps-and-solutions.md` A14–A18.

## Identity contract (the whole point)

```
Client → Gateway  Authorization: Bearer <JWT>
Gateway → Service  X-User-Id: <JWT sub>
                   X-User-Role: <JWT role>
```

1. The client sends `Authorization: Bearer <JWT>` to the **gateway only**.
2. The gateway validates the JWT with rally-security `JwtService.parseAndValidate`
   (RS256-signed, **not** encrypted, by the Auth Service's RSA private key; the gateway
   only holds the matching public key, `rally.jwt.public-key`, to verify).
3. The gateway **strips the token** and **injects `X-User-Id` / `X-User-Role`**
   before rerouting to the specific service.
4. Services never validate JWTs — they trust the injected headers (they must come
   only from the gateway). Client-supplied `X-User-Id` / `X-User-Role` are removed
   on every request, so they cannot be forged (gap A15/A16).

Every request also gets a fresh `X-Request-Id` tracing header (client-supplied values
are discarded) and consumes a token from a Redis-backed rate-limit bucket (§ Rate
limiting below).

## Dependencies

| Library | Version | What the gateway uses it for |
|---|---|---|
| `com.rally:rally-common` | `0.3.0-SNAPSHOT` | Exception vocabulary only — `ErrorResponse`, `UnauthenticatedException` |
| `com.rally:rally-security` | `0.1.1` | JWT — `JwtService`, `JwtProperties` (moved out of `rally-common`) |
| `org.springframework.boot:spring-boot-starter-data-redis-reactive` | (managed by Boot) | Redis client for the rate limiter |

JWT support used to live inside `rally-common`; it was split into the dedicated
`rally-security` package. The gateway depends on **both** now: `rally-security` for
token validation, `rally-common` for the shared error shape. Both must be installed
in the local Maven repo before building (see `GATEWAY-GUIDE.md` §6.1).

## Route table

| Route id | Paths | Service | Default URI |
|---|---|---|---|
| `auth` | `/auth/**`, `/users/**` | Auth | `http://localhost:8084` |
| `catalog` | `/products/**`, `/categories/**` | Catalog | `http://localhost:8083` |
| `participation` | `/deals/{id}/join, /leave, /participants, /progress, /invite-link`, `/invites/**` | Participation | `http://localhost:8086` |
| `deal` | `/deals/**` | Deal | `http://localhost:8085` |
| `order` | `/api/orders/**` | Order | `http://localhost:8081` |
| `payment` | `/api/payments/**`, `/api/users/{id}/payment-methods/**` | Payment | `http://localhost:8082` |
| `inventory` | `/inventory/**` | Inventory | `http://localhost:8087` |
| `notification` | `/notifications/**` | Notification | `http://localhost:8088` |

- Routes are evaluated **in order, first match wins**. The specific
  `/deals/*/join|leave|participants|progress|invite-link` predicates are listed
  **before** the broad `/deals/**` route so participation paths hit Participation,
  not Deal.
- Every URI is overridable via env (`CATALOG_SERVICE_URI`, ...).
- Auth/Deal/Participation/Inventory/Notification are not built yet — their routes
  are placeholders; requests will fail with 502 until the services exist.

## Public vs protected (gap A15)

Only these paths bypass JWT validation (`rally.gateway.public-paths`):

- `POST /auth/login`, `POST /auth/register`, `POST /auth/refresh`

Everything else — including `/auth/me` and all business endpoints — requires a valid
JWT and gets a `401` in the standard rally-common `ErrorResponse` shape otherwise.

## Rate limiting

Spring Cloud Gateway's built-in `RequestRateLimiter` filter (declared as a
**default filter**, so it covers every route) backed by a **Redis token bucket**:

- **Bucket key** — `config/RateLimitingConfig.java` picks the key per request:
  `user:<X-User-Id>` for authenticated calls, `ip:<remote-addr>` for the rest.
- **20 tokens/sec refill, burst of 40** — one request = one token
  (`replenishRate: 20`, `burstCapacity: 40`, `requestedTokens: 1`).
- Exhausted bucket → **`429 Too Many Requests`**.
- **Fails open** — if Redis is down, requests pass (limit temporarily skipped).

Redis is expected at `localhost:6379` (override via `REDIS_HOST` / `REDIS_PORT`) and
runs in the included compose file:

```bash
docker compose up -d redis   # then: docker exec rally-gateway-redis redis-cli ping → PONG
```

The integration tests drop the rate-limiter default filter, so `mvn test` needs **no
Redis**. Full line-by-line explanation: `GATEWAY-GUIDE.md` §5.4 + §13.12.

## Run

```bash
cd rally-gateway
mvn spring-boot:run        # gateway on :8080
```

Docker (services expected on the host):

```bash
docker compose up -d --build
```

Env vars worth knowing:

| Var | Default | Meaning |
|---|---|---|
| `GATEWAY_PORT` | `8080` | gateway port |
| `JWT_SECRET` | from `.env`, no default | must match every service; loaded via `spring.config.import` |
| `CATALOG_SERVICE_URI` / `ORDER_SERVICE_URI` / ... | `http://localhost:<port>` | downstream URIs |
| `CORS_ORIGIN_DEV` | `http://localhost:5173` | allowed Angular origin |
| `CORS_ORIGIN_PROD` | `http://localhost:4200` | allowed Angular origin |
| `REDIS_HOST` | `localhost` | Redis host for the rate limiter |
| `REDIS_PORT` | `6379` | Redis port for the rate limiter |

## Test

```bash
mvn test   # no JWT_SECRET needed — unit tests generate a random key, integration
           # tests load .env through the same spring.config.import the app uses
```

- `filter/JwtAuthGlobalFilterTest` — header injection / stripping / 401s (unit).
- `filter/FallbackGlobalFilterTest` — downstream failure → clean 503 JSON, and the
  already-committed response → original error re-thrown (unit).
- `filter/RequestIdGlobalFilterTest` — `X-Request-Id` injected, client values replaced (unit).
- `config/RateLimitingConfigTest` — rate-limit key strategy: `user:<id>` vs `ip:<addr>` (unit).
- `RouteConfigTest` — the full route table and first-match routing (incl. deal vs
  participation).
- `GatewayRoutingIntegrationTest` — end-to-end against a stub downstream: valid JWT →
  `X-User-Id`/`X-User-Role` injected, token + spoofed headers never forwarded, missing
  token → 401, public path passthrough, CORS preflight.
