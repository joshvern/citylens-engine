"use strict";

const { createHash } = require("node:crypto");

const NO_FAILURES = "The verifier exited before recording a failure list.";

function failureSignature(failures) {
  // Age increases on every scheduled check. Keep source names, thresholds,
  // counts, HTTP statuses, and all other evidence significant.
  const normalized = [
    ...new Set(
      (failures.length ? failures : [NO_FAILURES]).map((failure) =>
        String(failure)
          .replace(/\r?\n/g, " ")
          .replace(/\b\d+(?:\.\d+)? days old\b/g, "<age> days old")
          .trim(),
      ),
    ),
  ].sort();
  return createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
}

function failureMarker(failures) {
  return `<!-- citylens-failures:v1:${failureSignature(failures)} -->`;
}

function previousSignature(body) {
  const signature = body.match(/<!-- citylens-failures:v1:([a-f0-9]{64}) -->/);
  if (signature) return signature[1];

  // Migrate existing incidents without one last duplicate notification. Older
  // bodies list at most 20 failures; an incomplete match alerts conservatively.
  const section = body.match(
    /### Verifier failures\r?\n\r?\n((?:- [^\r\n]+\r?\n?)+)/,
  );
  if (!section) return null;
  const failures = section[1]
    .trim()
    .split(/\r?\n/)
    .map((line) => line.slice(2));
  return failureSignature(failures);
}

function shouldComment(previousBody, failures, runToken) {
  const body = String(previousBody || "");
  return (
    !body.includes(runToken) &&
    previousSignature(body) !== failureSignature(failures)
  );
}

module.exports = { failureMarker, failureSignature, shouldComment };
