# Firestore restore drill — 2026-07-25

Status: **accepted and cleaned up**

This drill proved that the production Firestore backup is restorable, that the
restored application state and index contract are present, and that the
CityLens runtime identities cannot access a same-project recovery database.
No document values or user identifiers were exported as drill evidence.

## Source and operation

- project: `citylens-001`
- source database: `(default)`
- backup:
  `projects/citylens-001/locations/us-central1/backups/481013f5-7c83-434d-acba-cfe0218a22f5`
- snapshot: `2026-07-25T04:41:30.976224Z`
- backup expiry: `2026-08-08T04:41:30.976224Z`
- drill database: `recovery-drill-20260725-0507`
- restore operation:
  `projects/citylens-001/databases/recovery-drill-20260725-0507/operations/rzSNhCzaMYRISmJ4voxsXBAqMWxhcnRuZWMtc3ULIgoQIRo`
- started: `2026-07-25T05:06:41.425530Z`
- completed: `2026-07-25T05:16:33.768312Z`
- duration: `592.343` seconds
- terminal state: `SUCCESSFUL`, 100% completed

The restored database retained delete protection. PITR was disabled on the
temporary database, as expected for a new restore target.

## IAM isolation

Before the restore, the API and worker's unconditional project-level
`roles/datastore.user` grants were replaced with one conditional binding:

```text
resource.name == "projects/citylens-001/databases/(default)"
```

The version-2 runtime IAM verifier passed with zero failures or warnings.
REST probes through temporary, service-account-level impersonation produced:

| Runtime identity | `(default)` | drill database |
| --- | ---: | ---: |
| `citylens-api` | HTTP 200 | HTTP 403 |
| `citylens-worker` | HTTP 200 | HTTP 403 |

The temporary operator token-creator grants were removed after the probes.
The API service account's required self-signing token-creator grant remains;
the worker has no token-creator grant.

## Data and index evidence

Both databases returned the same complete root collection set:

```text
api_keys_by_hash, auth_identities, runs, usage_months, users
```

Metadata-only, limit-one queries also produced identical presence results:

| Collection or collection group | `(default)` | restored |
| --- | ---: | ---: |
| `api_keys_by_hash` | present | present |
| `auth_identities` | present | present |
| `runs` | present | present |
| `usage_months` | present | present |
| `users` | present | present |
| `artifacts` | present | present |
| `product_usage_days` | present | present |
| `api_keys` | present | present |
| `parcel_workflow` | empty | empty |
| `parcel_saved_searches` | empty | empty |
| `events` | empty | empty |

The four composite index definitions matched exactly after excluding generated
resource names and lifecycle state. Both canonical contracts produced:

```text
sha256:229e86eb6540e07c440fddf31937de2f1c0e075e8271fa1f51ecfa953dc40f23
```

## TTL and rules

As documented by Firestore, TTL policies were not present immediately after
restore. The two production policies were explicitly reapplied and reached
`ACTIVE` before cleanup:

- `pilot_requests.expires_at`
- `product_usage_days.expires_at`

The project has one Firebase Rules release, `cloud.firestore`. CityLens web
clients do not import or use the Firestore client SDK; application access is
server-side and governed by IAM. A future client-direct Firestore cutover would
still require an explicit per-database Rules deployment because Rules are not
contained in the backup.

## Cleanup and final gates

The cleanup command refused names outside `recovery-drill-*` and explicitly
refused `(default)`. It then:

1. disabled delete protection only on `recovery-drill-20260725-0507`;
2. read the updated etag;
3. submitted an etag-bound delete;
4. confirmed the named database was absent.

Deletion completed at `2026-07-25T05:24:06.821804Z`. The final database list
contained only `(default)`, still with delete protection and PITR enabled.

Post-cleanup acceptance at `2026-07-25T05:24Z`:

- runtime IAM verifier: healthy, zero failures/warnings
- production recovery verifier: `ready`, one fresh READY backup, zero
  failures/warnings
- public production verifier: 18/18 checks, zero failures/warnings, zero
  source-SLA breaches
- API and worker ERROR logs after IAM cutover: none
