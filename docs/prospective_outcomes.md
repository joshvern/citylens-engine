# Prospective workflow outcome measurement

CityLens keeps two evaluation questions separate:

1. The parcel model's historical forward test asks whether a tax lot receives a
   later DOB new-building job filing.
2. The private workflow measurement asks whether a user-saved lead advances
   after it enters that user's pipeline.

Workflow outcomes are not model accuracy, seller intent, or a probability that
a parcel will transact.

## Fixed-horizon contract

The authenticated
`GET /v1/parcel-intel/workflow/analytics` response uses immutable `saved_at` as
the start of observation and reports:

| Milestone | Observation window |
|---|---:|
| Owner contacted | 30 days |
| Qualified | 90 days |
| Offer submitted | 180 days |
| Under contract | 270 days |
| Closed | 365 days |

A lead enters a window's denominator only after the entire window has elapsed.
The numerator requires an explicit first-recorded milestone timestamp no later
than `saved_at + horizon_days`. A legacy inferred outcome can appear in the
operational lifetime funnel, but it cannot increase a fixed-horizon rate.
Likewise, a milestone entered after its deadline is retained in the audit trail
but is not retroactively treated as an on-time outcome.

Archived leads remain in denominators. This prevents users from improving a
rate by deleting unfavorable outcomes.

## Evidence thresholds

- Fewer than 10 mature records: the corresponding rate is labeled
  `Collecting`.
- Fewer than 30 saved records, or no window with 10 mature records: overall
  status is `collecting`.
- At least one directional window, but incomplete event/rank coverage or fewer
  than 30 fully observed 365-day records: status is `directional`.
- At least 30 fully observed 365-day records and at least 80% immutable event
  and rank-snapshot coverage: status is `usable`.

Rank-band cohort rates use the same fixed horizons and expose their own
denominators. Raw lifetime milestone counts remain available only as
operational funnel context.

## Selection and timing limitations

The population is user-saved leads, not every parcel shown or ranked by
CityLens. User selection and outreach effort can differ by rank band, so cohort
comparisons are observational and should not be interpreted causally.
Milestone timestamps mean "first recorded in CityLens." Late data entry is
handled conservatively and may undercount an outcome reached on time but
recorded later.

The public, data-free
`GET /v1/parcel-intel/workflow/analytics/methodology` endpoint exposes the
schema version, horizons, thresholds, and non-accuracy disclaimer. The
production verifier fails if that deployed contract changes unexpectedly.

## Follow-up completeness

Prospective measurement depends on timely workflow use, so open records may
store a structured `next_action` and `next_action_due_date`. The authenticated
`GET /v1/parcel-intel/workflow/actions` endpoint derives a private action queue
on the server:

- overdue, due today, due within seven days, scheduled, or unscheduled;
- missing assignee;
- outcome update due after 30 days with no recorded outcome.

A due date without a concrete action is rejected. Closing, rejecting, losing,
or passing on a lead clears its stale reminder and removes it from the open
queue. These operational completeness states do not change ranks, fabricate
outcomes, or relax the fixed-horizon measurement rules.
