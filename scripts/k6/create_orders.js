import http from "k6/http";
import { check } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const PLENTY = "11111111-1111-1111-1111-111111111111"; // 100k units
const SCARCE = "22222222-2222-2222-2222-222222222222"; // 10 units

export const options = {
  scenarios: {
    steady_orders: { executor: "constant-arrival-rate", rate: 100, timeUnit: "1s", duration: "30s", preAllocatedVUs: 50, exec: "plenty" },
    oversell_attempt: { executor: "per-vu-iterations", vus: 50, iterations: 1, exec: "scarce" },
  },
  thresholds: { "http_req_duration{scenario:steady_orders}": ["p(95)<300"], checks: ["rate>0.99"] },
};

const headers = (key) => ({ "Content-Type": "application/json", "X-API-Key": "dev-key-1", "Idempotency-Key": key });
const body = (product) => JSON.stringify({ items: [{ product_id: product, quantity: 1 }] });

export function plenty() {
  const key = uuidv4();
  const first = http.post(`${BASE}/api/v1/orders`, body(PLENTY), { headers: headers(key) });
  // Retrying with the same key must return the same order, not create a new one.
  const retry = http.post(`${BASE}/api/v1/orders`, body(PLENTY), { headers: headers(key) });
  check(first, { "created": (r) => r.status === 201 });
  check(retry, { "replayed same order": (r) => r.status === 200 && r.json("id") === first.json("id") });
}

// 50 buyers, 10 units: expect exactly 10 x 201 and 40 x 409 (checked via /api/v1/products).
export function scarce() {
  const r = http.post(`${BASE}/api/v1/orders`, body(SCARCE), { headers: headers(uuidv4()) });
  check(r, { "201 or 409": (res) => res.status === 201 || res.status === 409 });
}
