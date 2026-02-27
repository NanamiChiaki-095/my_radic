import http from "k6/http";
import { check } from "k6";
import { Rate, Trend } from "k6/metrics";

const targetBase = __ENV.TARGET_BASE || "http://gateway.radic.svc.cluster.local:8080";
const endpoint = __ENV.SEARCH_ENDPOINT || "/search_http";
const query = __ENV.SEARCH_QUERY || "golang";
const duration = __ENV.DURATION || "2m";
const rate = Number(__ENV.RATE || 2000);
const preAllocatedVUs = Number(__ENV.PRE_ALLOCATED_VUS || 400);
const maxVUs = Number(__ENV.MAX_VUS || 2000);
const requestTimeout = __ENV.REQUEST_TIMEOUT || "15s";
const parseJSON = (__ENV.PARSE_JSON || "0") === "1";

const fullURL = `${targetBase}${endpoint}?q=${encodeURIComponent(query)}`;

export const errRate = new Rate("http_error_rate");
export const appLatency = new Trend("app_latency_ms", true);

export const options = {
	scenarios: {
		search_http: {
			executor: "constant-arrival-rate",
			duration,
			rate,
			timeUnit: "1s",
			preAllocatedVUs,
			maxVUs,
		},
	},
	thresholds: {
		http_req_failed: ["rate<0.05"],
		http_error_rate: ["rate<0.05"],
		http_req_duration: ["p(95)<300", "p(99)<1500"],
	},
	discardResponseBodies: false,
	summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export default function () {
	const res = http.get(fullURL, {
		tags: { name: "search_http" },
		timeout: requestTimeout,
	});

	const ok = check(res, {
		"status is 200": (response) => response.status === 200,
		"body not empty": (response) => response.body && response.body.length > 0,
	});
	errRate.add(!ok);
	appLatency.add(res.timings.duration);

	if (parseJSON && res.status === 200) {
		try {
			JSON.parse(res.body);
		} catch (err) {
			errRate.add(true);
		}
	}
}

export function handleSummary(data) {
	const out = {
		target: fullURL,
		rate,
		duration,
		preAllocatedVUs,
		maxVUs,
		http_reqs: data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0,
		http_req_failed_rate: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.rate : 0,
		http_req_duration_p95_ms: data.metrics.http_req_duration ? data.metrics.http_req_duration.values["p(95)"] : 0,
		http_req_duration_p99_ms: data.metrics.http_req_duration ? data.metrics.http_req_duration.values["p(99)"] : 0,
	};

	return {
		stdout: JSON.stringify(out, null, 2) + "\n",
	};
}
