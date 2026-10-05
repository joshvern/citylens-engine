"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { createRequire } = require("node:module");
const { test } = require("node:test");
const {
  failureMarker,
  failureSignature,
  shouldComment,
} = require("../production_incident.cjs");

const root = path.resolve(__dirname, "../..");
const workflow = fs.readFileSync(
  path.join(root, ".github/workflows/production-smoke.yml"),
  "utf8",
);
const marker = "<!-- citylens-incident:production-smoke -->";
const previousFailure =
  "public: index: source SLA ownership is stale (82.5 days old; limit 45.0)";
const currentFailure =
  "public: index: source SLA ownership is stale (82.8 days old; limit 45.0)";

function legacyBody(failures) {
  return [
    marker,
    "### Verifier failures",
    "",
    ...failures.map((value) => `- ${value}`),
    "",
    "Treat this as a production incident.",
  ].join("\n");
}

function scriptFor(name) {
  const step = workflow
    .split(`      - name: ${name}\n`)[1]
    .split("\n      - name: ")[0];
  return step
    .split("          script: |\n")[1]
    .split("\n")
    .map((line) => line.slice(12))
    .join("\n");
}

async function runAction({
  failures = [currentFailure],
  previousBody,
  close = false,
} = {}) {
  const calls = [];
  const reports = Object.fromEntries(
    [
      "production-verification.json",
      "demo-artifact-verification.json",
      "authenticated-production-verification.json",
      "browser-authenticated-production-verification.json",
    ].map((name, index) => [
      name,
      {
        schema_version: "test@v1",
        failures: index
          ? []
          : failures.map((value) => value.replace(/^public: /, "")),
      },
    ]),
  );
  const github = {
    paginate: async () =>
      previousBody === undefined
        ? []
        : [
            {
              number: 150,
              title: "[Production] Scheduled verification failing",
              body: previousBody,
            },
          ],
    rest: {
      issues: Object.fromEntries(
        ["listForRepo", "create", "createComment", "update"].map((method) => [
          method,
          async (args) => calls.push({ method, ...args }),
        ]),
      ),
    },
  };
  const localRequire = createRequire(path.join(root, "package.json"));
  const mockedRequire = (name) =>
    name === "fs"
      ? {
          existsSync: (filename) => filename in reports,
          readFileSync: (filename) => JSON.stringify(reports[filename]),
        }
      : localRequire(name);
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const action = new AsyncFunction(
    "require",
    "github",
    "context",
    "process",
    "core",
    scriptFor(
      close
        ? "Close production incident after recovery"
        : "Open or update production incident",
    ),
  );
  await action(
    mockedRequire,
    github,
    { repo: { owner: "test", repo: "engine" }, runId: 2, sha: "test-commit" },
    {
      env: { GITHUB_SERVER_URL: "https://github.com", GITHUB_RUN_ATTEMPT: "1" },
    },
    { warning: () => {} },
  );
  return calls;
}

test("ages, order, and duplicates do not change incident identity", () => {
  assert.equal(
    failureSignature([
      previousFailure,
      "public: readiness: parcel feed is stale",
    ]),
    failureSignature([
      "public: readiness: parcel feed is stale",
      currentFailure,
      currentFailure,
    ]),
  );
  assert.equal(
    failureSignature(["index: feed is 67.1 days old (limit 35.0)"]),
    failureSignature(["index: feed is 67.4 days old (limit 35.0)"]),
  );
  assert.equal(
    failureSignature(["index: land-use project details are 70.1 days old"]),
    failureSignature(["index: land-use project details are 70.4 days old"]),
  );
});

test("sources, thresholds, counts, and HTTP status remain significant", () => {
  for (const changed of [
    currentFailure.replace("ownership", "project_activity"),
    currentFailure.replace("45.0", "8.0"),
    "public: returned 4999 rows",
    "public: HTTP 503",
  ]) {
    assert.notEqual(
      failureSignature([currentFailure]),
      failureSignature([changed]),
    );
  }
  assert.notEqual(
    failureSignature(["public: returned 4999 rows"]),
    failureSignature(["public: returned 4998 rows"]),
  );
  assert.notEqual(
    failureSignature(["public: HTTP 503"]),
    failureSignature(["public: HTTP 401"]),
  );
});

test("same run remains idempotent and missing previous evidence alerts", () => {
  assert.equal(
    shouldComment(
      "<!-- citylens-run:2:1 -->",
      [currentFailure],
      "<!-- citylens-run:2:1 -->",
    ),
    false,
  );
  assert.equal(
    shouldComment(marker, [currentFailure], "<!-- citylens-run:2:1 -->"),
    true,
  );
});

test("new failures beyond the 20 displayed rows are significant", () => {
  const failures = Array.from(
    { length: 21 },
    (_, index) => `public: failure ${index}`,
  );
  assert.equal(
    shouldComment(
      failureMarker(failures),
      [...failures, "public: new outage"],
      "new-run",
    ),
    true,
  );
  assert.equal(
    shouldComment(legacyBody(failures.slice(0, 20)), failures, "new-run"),
    true,
  );
});

test("new incident creates one issue with full failure signature", async () => {
  const calls = await runAction();
  assert.deepEqual(
    calls.map((call) => call.method),
    ["create"],
  );
  assert.ok(calls[0].body.includes(failureMarker([currentFailure])));
});

test("unchanged legacy incident migrates silently and updates latest receipt", async () => {
  const calls = await runAction({
    previousBody: legacyBody([previousFailure]),
  });
  assert.deepEqual(
    calls.map((call) => call.method),
    ["update"],
  );
  assert.ok(calls[0].body.includes(failureMarker([currentFailure])));
  assert.ok(calls[0].body.includes("<!-- citylens-run:2:1 -->"));
});

test("unchanged signed incident updates without a duplicate comment", async () => {
  const calls = await runAction({
    previousBody: `${marker}\n${failureMarker([previousFailure])}`,
  });
  assert.deepEqual(
    calls.map((call) => call.method),
    ["update"],
  );
});

test("changed failures emit one notification and update the incident", async () => {
  const calls = await runAction({
    previousBody: legacyBody([previousFailure]),
    failures: [currentFailure, "public: API HTTP 503"],
  });
  assert.deepEqual(
    calls.map((call) => call.method),
    ["createComment", "update"],
  );
  assert.match(calls[0].body, /Production failures changed/);
});

test("recovery still emits one notification and closes the incident", async () => {
  const calls = await runAction({
    previousBody: legacyBody([previousFailure]),
    close: true,
  });
  assert.deepEqual(
    calls.map((call) => call.method),
    ["createComment", "update"],
  );
  assert.match(calls[0].body, /Production verification recovered/);
  assert.equal(calls[1].state, "closed");
});
