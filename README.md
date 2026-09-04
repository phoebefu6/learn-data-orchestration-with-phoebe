# Learn Data Orchestration with Phoebe

Six 45-minute sessions on running scheduled data pipelines reliably: cron to DAGs,
retries, idempotency, backfills and outcome alerting. The running artifact is Daybreak's
2am nightly warehouse build - the third course set in the same world as
[Learn Data Pipelines](https://phoebefu6.github.io/learn-data-pipelines-with-phoebe/) and
[Learn Streaming Data](https://phoebefu6.github.io/learn-streaming-data-with-phoebe/).

**Live:** https://phoebefu6.github.io/learn-data-orchestration-with-phoebe/

## Thirty nights at Daybreak

Every session upgrades the same simulator: thirty deterministic nights of a 7-task DAG
with a 7am deadline. Bare cron publishes 30/30 mornings on time and 12/30 silently wrong.
DAG dependencies, retries, idempotent tasks, a backfill and outcome alerting earn the
numbers back - including one honest dip (retries buy availability and quietly double-write
until idempotency lands) and an anti-lever ("retry everything 10x") whose only measurable
effect is delaying the pager by three and a half hours. The failure model is a seeded
teaching script; the scheduling arithmetic is computed live.

## Structure

| # | Session | Difficulty |
|---|---------|------------|
| 1 | The 2am job | easy |
| 2 | From cron to DAG | easy |
| 3 | Retries | medium |
| 4 | Idempotency | medium |
| 5 | Backfills | hands-on |
| 6 | Run it like production | hands-on |

Sources and the verified fact base live in `materials/official-course-map.md`.

by Phoebe Fu · part of [Learn with Phoebe](https://phoebefu6.github.io/learn-with-phoebe/)
