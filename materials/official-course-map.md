# learn-data-orchestration-with-phoebe - official source map

Built 2026-09-04. Single-track 6 sessions, scheduler-agnostic core (Airflow named as the
reference tool, Dagster used to prove the concepts transfer). Running artifact: Daybreak's
2am nightly warehouse build (third course in the Daybreak world after learn-data-pipelines
and learn-streaming-data). Difficulty d2 (Core) - fills the deng bucket's missing rung
between Pipelines (d1) and the d3 shelf.

Positioning seam: learn-dataops-with-phoebe (data bucket, d4) runs orchestration ideas at
MLOps/production scale (drift, retrain triggers); this course is the d2 foundations that
sit under it. Do not duplicate its "Idempotency and backfills" session framing - here
idempotency and backfills each get a full session at on-ramp depth.

Fast-moving product note: Airflow docs read at version 3.3.1 on 2026-09-04. The Airflow 3
changes below are load-bearing - re-verify before any live delivery.

## Simulator canon (orchestra-live.js, seed 20260904, spike 3.5x - VERIFIED LIVE 2026-09-04)

Thirty nights of Daybreak's 7-task DAG (3 extracts -> clean -> join -> aggregate -> publish),
dashboard due 7:00am. Scripted in every mode: a logic bug in aggregate lives days 10-13, and
extract_payments hard-breaks on day 14 (retries cannot fix it).

| Rung | Mornings on time | Silently wrong | Other counters |
|------|-----------------|----------------|----------------|
| Bare cron | **30/30** | **12/30** | history 26/30 - the punctual liar |
| + DAG dependencies | 23/30 | 4/30 | 6 never published, 1 late - honesty first, availability suffers |
| + Retries (2, backoff) | 28/30 | **6/30 - a real dip** | 2 nights double-wrote (crashed writer had already appended) |
| + Idempotent tasks | 28/30 | 4/30 | double-writes 0 - overwrite the partition, not append |
| + Backfill (needs idempotency; passport-gated) | 28/30 | 4/30 | history 26/30 -> **30/30** |
| + Alert on outcomes | 28/30 | 4/30 | day-14 detection 9h 4m -> **12 min** |
| Anti-lever: retry everything 10x | 28/30 | 4/30 | detection 12 min -> **3h 28m** - nothing else moves |

Teaching beats the numbers carry:
- Bare cron is on time 30/30 and wrong 12/30: punctuality is not correctness.
- The DAG rung makes things honestly WORSE on availability (23/30): failed nights become
  visible missing mornings instead of quiet wrong ones.
- The retries rung is the course's honest dip: availability comes back (28/30) while
  silently-wrong RISES 4 -> 6, because a crashed writer had already appended half its rows
  and the retry appends them again. The fix is not fewer retries - it is idempotency.
- Full-ladder residuals stay visible and are the point: 1 morning never published (the
  day-14 hard break - no retry policy fixes broken code), 1 late (volume spike), 4 mornings
  that WERE wrong while the bug lived (backfill restates history; it cannot un-ring the
  morning).
- The anti-lever moves exactly one number, in the wrong direction: ten retries add 3h16m of
  retry storm in front of the page. Anchored on real evidence (SRE book + AWS below).
- Failure model = seeded simulation (badged); dependency resolution, retry/rerun arithmetic
  and the 30-day tallies = computed live (badged measured).

## Per-session coverage

### Session 1 - The 2am job (cron, and what it never promised)
| Source | Coverage | What |
|--------|----------|------|
| man7 crontab(5) | ✓ | verbatim: commands executed "when the 'minute', 'hour', and 'month of the year' fields match the current time"; cron "examines cron entries every minute"; DST missing-hours jobs "not to be run" / repeated times "run twice" |
| man7 cron(8) | ✓ | small clock jumps compensated (<=3h); /etc/cron.d jobs "now run through anacron" |
| Feature-absence claims | ◐ | no dependencies, no retry, no overlap prevention, no downtime catch-up: verified as ABSENCE from the man pages, not as verbatim statements - phrase as "cron defines no such feature; anacron exists for downtime catch-up". Never quote a sentence for these. |

### Session 2 - From cron to DAG
| Source | Coverage | What |
|--------|----------|------|
| Airflow core-concepts: DAGs (3.3.1) | ✓ | verbatim: "A Dag is a model that encapsulates everything needed to execute a workflow"; "dictates the order in which they have to run, and which tasks depend on what others"; deps declared with >> and << |
| Airflow core-concepts: DAG Runs | ✓ | data interval: "Each Dag run in Airflow has an assigned 'data interval' that represents the time range it operates in"; logical date "denotes the start of the data interval, not when the Dag is actually executed" |
| Dagster concepts | ✓ | verbatim one-liners: asset = "An asset represents a logical unit of data such as a table, dataset, or machine learning model"; schedule = "A ScheduleDefinition is a way to automate jobs or assets to occur on a specified interval"; sensor = "A sensor is a way to trigger jobs or assets when an event occurs"; job = "a subset of assets or the GraphDefinition of ops" - the same ideas under different names (scheduler-agnostic proof) |

### Session 3 - Retries
| Source | Coverage | What |
|--------|----------|------|
| Airflow Tasks (3.3.1) | ✓ | retries = max retry count; retry_delay / retry_exponential_backoff / max_retry_delay exist; task state up_for_retry; execution_timeout |
| Google SRE book ch22 (Addressing Cascading Failures) | ✓ | verbatim: "retries can destabilize a system"; "Don't retry a given request indefinitely"; retry budget example "only allow 60 retries per minute"; "Always use randomized exponential backoff"; amplification "as large as the product of the number of attempts at each layer"; 100 -> 200 -> 300 QPS growth |
| AWS Builders' Library: Timeouts, retries, backoff with jitter | ✓ | verbatim: "Retries are 'selfish'"; "similar to a powerful medicine"; layered "243x"; token bucket in the AWS SDK since 2016; "retries can mask those failures" (cite the article; page byline shows only "Marc, AWS Employee" - do not print a surname) |
| Dagster op retries | ✓ | RetryPolicy with delay, Backoff.EXPONENTIAL, Jitter |

### Session 4 - Idempotency
| Source | Coverage | What |
|--------|----------|------|
| Beauchemin, Functional Data Engineering (Medium 2018) | ✓ | verbatim: "A pure task should be deterministic and idempotent, meaning that it will produce the same result every time it runs or re-runs"; "should always fully overwrite a partition as its output"; "Thinking of partitions as immutable blocks of data and systematically overwriting partitions is the way to make your tasks functional"; "Having idempotent tasks is vital for the operability of pipelines"; "immutable data along with versioned logic are key to reproducibility". Author self-identifies in-article as "the creator of Airflow"; Wikipedia infobox corroborates (Maxime Beauchemin / Airbnb, started Oct 2014). Retrieved via reader proxy of maximebeauchemin.medium.com (direct fetch 403). NOTE: the literal string "INSERT OVERWRITE" does NOT appear in the article - do not attribute it. |
| Airflow best-practices (3.3.1) | ✓ | verbatim: "treat tasks in Airflow equivalent to transactions in a database"; "never produce incomplete results"; "Do not use INSERT during a task re-run, an INSERT statement might lead to duplicate rows in your database. Replace it with UPSERT"; now() "should never be used inside a task"; "Never read the latest available data in a task"; "Read and write in a specific partition. You can use data_interval_start as a partition" |

### Session 5 - Backfills
| Source | Coverage | What |
|--------|----------|------|
| Airflow DAG Runs: catchup + backfill (3.3.1) | ✓ | verbatim: catchup=True -> "the scheduler will kick off a Dag Run for any data interval that has not been run since the last data interval (or has been cleared)"; backfill = "re-run all the instances of the dag_id for all the intervals within the start date and end date"; Airflow 3 CLI shape: airflow backfill create --dag-id --from-date --to-date --reprocess-behavior |
| Beauchemin 2018 | ✓ | verbatim: "all of the data structures derived from that table, for that time range, need to be reprocessed, or backfilled"; "apply the exact same compute scheme as we did on the original pass, on top of the new, corrected data" |
| Dagster backfills | ✓ | "Backfilling is the process of running partitions for assets that either don't exist or updating existing records" |

### Session 6 - Run it like production
| Source | Coverage | What |
|--------|----------|------|
| Airflow Tasks (3.3.1) - SLA status | ✓ | verbatim, load-bearing: "The SLA feature from Airflow 2 has been removed in 3.0 and was replaced in Airflow 3.1 with Deadlines Alerts." Teach outcome deadlines generically; never teach sla= / sla_miss_callback as current. |
| Google SRE book ch6 (Monitoring Distributed Systems) | ✓ | verbatim: symptom vs cause ("The 'what's broken' indicates the symptom; the 'why' indicates a... cause"); black-box monitoring "symptom-oriented... 'The system isn't working correctly, right now'"; "it's better to spend much more effort on catching symptoms than causes"; four golden signals "latency, traffic, errors, and saturation"; "Every page should be actionable"; white-box monitoring detects "failures masked by retries" - the anti-lever's anchor quote |
| dbt source freshness | ✓ | freshness block with warn_after/error_after + loaded_at_field; "dbt uses the freshness properties to construct a select query"; deploy docs tie it to "the service level agreement (SLA) that you've defined" |
| SRE ch22 + AWS (from session 3) | ◐ | recap for the anti-lever verdict |

## Not covered by design
- Airflow operators/sensors/executors, TaskFlow API, deployment (drifts to d3 and
  tool-locks; learn-data-engineering covers warehouse-side depth)
- Kubernetes/celery execution layers, Airflow control plane
- Streaming triggers (learn-streaming-data owns real-time; this course is batch cadence)
- MLOps-scale operation: drift monitoring, retrain triggers (learn-dataops territory)

## Verification gaps (recorded honestly)
- Cron's "no retry / no dependency / no overlap prevention / no downtime catch-up" are
  argument-from-absence findings on man7 crontab(5)/cron(8), not quotable sentences.
- Beauchemin quotes came via reader proxy, triple-pass consistent, not byte-diffed
  against Medium's rendered page (direct fetch 403).
- AWS article byline verified only as "Marc, AWS Employee" - cite the article title.
- Airflow 2's sla= parameter semantics were NOT fetched (only the 3.x removal notice).
- SRE "Handling Overload" chapter (per-request retry budgets) not fetched; ch22's retry
  budget quote is the one to use.
