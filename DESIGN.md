# Retry with Back-off for Otter

Retrying is an excellent idea for Otter! I have experience with Apache Airflow where it is built-in, and it is much needed functionality.

Tasks without retries waste human time because when `run()` fails for the first time due to transient issue that could easily be fixed with retry, someone has to run the entire task again and it introduces delay between noticing task failed and rerunning, context switching etc, when this could be entirely automated. When a task fails the first instinct is to retry, so when retries are built in the pipeline, and tasks fail, you know it is not a transient issue and you have to debug.

---

## 1. How It Would Work

### Config Schema

Here is how I see retry config, based on simple example in `config.yaml` file:

```yaml
steps:
  simple:
    - name: hello_world simple
      who: simple
      retry:
        run:
          retries: 1
          backoff: exponential
          initial_delay: 30
        validation:
          retries: 2
          backoff: fixed
          initial_delay: 10
```

### Implementation

I added `retry.py` defining separate retry policies for run and validation and imported `RetryConfig` in `model.py`, so now retry gets parsed in `Spec` and old examples with no retries are still parsed. Work for the future: now if retries are set to 0, other arguments (`backoff`, `initial_delay`, `max_delay`, `jitter`) are still default values, which doesn't make sense. It is fixable with adding another layer after retry but I am out of time.

`retry.py` intentionally imports nothing from within Otter. The dependency graph is `model.py` to `retry.py` to pydantic only. This avoids a circular import between `retry.py` and `model.py` that would occur if `RetryConfig` knew about `Spec` or `Task`.

```python
>>> from otter.task.model import Spec

>>> s = Spec(**{
...     "name": "hello_world simple",
...     "who": "simple",
...     "retry": {
...         "run": {"retries": 1, "backoff": "exponential", "initial_delay": 30},
...         "validation": {"retries": 2, "backoff": "fixed", "initial_delay": 10}
...     }
... })
>>> s
Spec(name='hello_world simple', requires=[], scratchpad_ignore_missing=False, who='simple', retry={'run': {'retries': 1, 'backoff': 'exponential', 'initial_delay': 30}, 'validation': {'retries': 2, 'backoff': 'fixed', 'initial_delay': 10}})

>>> s2 = Spec(**{ "name": "hello_world simple", "who": "simple"})
>>> s2
Spec(name='hello_world simple', requires=[], scratchpad_ignore_missing=False, who='simple')
>>> s2.retry
RetryConfig(run=RetryPolicy(retries=0, backoff=<BackoffStrategy.EXPONENTIAL: 'exponential'>, initial_delay=5.0, max_delay=120.0, jitter=False), validation=RetryPolicy(retries=0, backoff=<BackoffStrategy.EXPONENTIAL: 'exponential'>, initial_delay=5.0, max_delay=120.0, jitter=False))
```

In `task_reporter.py`, I wrapped the function call in a loop and moved fail to the final attempt only.

Tested with `find-latest` — since I don't have credentials and it did go to retry!

```bash
otter -s find-latest -c config.yaml
```

---

### Execution Flow

**Before:**

```
start_run(): logs + updates manifest: started_run_at
  [run() executes]
finish_run(): logs + updates manifest: finished_run_at
  — OR —
fail(error): logs + updates manifest: result=FAILURE, failure_reason
```

The moment `task.fail()` is called, two things happen simultaneously: the manifest records a permanent failure, and the abort event fires, telling every other running task to stop. That's why retry can't happen after `task.fail()` — by then it's already declared dead.

**After:**

The model is based on how retry concept works on Airflow, which is:

```python
for attempt in range(max_attempts):
    try:
        task.execute()
        break  # success
    except Exception:
        if attempt < max_attempts - 1:
            sleep(backoff_delay)
            continue   # retry
        else:
            task.set_failed()  # NOW it's permanently failed
            raise
```

I added the same check in `task_reporter.py`:

```python
except Exception as e:
    if attempt < policy.retries:
        logger.warning(
            f'task {name} failed (attempt {attempt + 1}/{policy.retries + 1}), '
            f'retrying in {policy.initial_delay}s'
        )
        await asyncio.sleep(policy.initial_delay)
    else:
        self.context.abort.set()
        if isinstance(e, TaskAbortedError):
            self.abort()
        else:
            self.fail(e, name)
        return self
```

---

### State Transitions

Retry introduces no new states. The existing state machine (`PENDING_RUN → RUNNING → PENDING_VALIDATION → VALIDATING → DONE`) is unchanged. The retry loop runs entirely within `RUNNING` or `VALIDATING`, so from the coordinator's perspective a retrying task looks identical to the first attempt of a task.

---

### Manifest Behaviour

Each task updates the manifest via `Step.upsert_task_manifest()`. Retry attempts need new fields here to be observable.

> **To be done:** updating manifest JSON file to respect FAIR principle and log all run and validation attempts.

```json
{
    "name": "find_latest cosmic",
    "result": "failure",
    "started_run_at": "2026-04-27T14:45:44.988880Z",
    "finished_run_at": "2026-04-27T14:46:12.377585Z",
    "started_validation_at": null,
    "finished_validation_at": null,
    "run_attempts": [
        {
            "attempt": 1,
            "started_at": "2026-04-27T14:45:44.988880Z",
            "finished_at": "2026-04-27T14:45:57.112233Z",
            "failure_reason": "503 Service Unavailable"
        },
        {
            "attempt": 2,
            "started_at": "2026-04-27T14:46:02.112233Z",
            "finished_at": "2026-04-27T14:46:12.377585Z",
            "failure_reason": "403 Forbidden: no credentials"
        }
    ],
    "validation_attempts": [],
    "failure_reason": "403 Forbidden: no credentials",
    "log": [
        "INFO    | find_latest cosmic :: run() attempt 1/2 starting",
        "WARNING | find_latest cosmic :: run() attempt 1/2 failed: 503 Service Unavailable. Retrying in 5s (backoff=fixed)",
        "INFO    | find_latest cosmic :: run() attempt 2/2 starting",
        "ERROR   | find_latest cosmic :: run() attempt 2/2 failed: 403 Forbidden: no credentials. No retries left"
    ]
}
```

---

## 2. Alternatives Considered

### Retrying only `run()`

If `run()` succeeds but `validate()` fails due to a transient issue (e.g., checking if a file was correctly written to a cloud bucket), the whole step still aborts.

### Retrying only `validate()`

This is insufficient because the most common failures (API/HTTP errors) occur during the execution phase (`run`). Retrying only `validate()` would waste human time because when `run()` fails for the first time due to transient issue that could easily be fixed with retry, someone has to run the entire task again and it introduces delay between noticing task failed and rerunning, context switching etc, when this could be entirely automated. Generally when a task fails the first instinct is to retry, so when retries are built in the pipeline, and tasks fail, you know it is not a transient issue and you have to debug.

### Retrying `run()` + `validate()` as an atomic unit

I considered a single list of attempts where each entry includes both run and validation status. However, this is harder to query if you want to know "Which stage is the bottleneck?". Transitioning to an Airflow-style setup where `run()` and `validate()` are treated as independent, retryable units directly addresses the concern that re-running an entire step is "expensive". By separating them, if a task successfully completes a 2-hour data processing `run()` but fails a `validate()` check due to a transient cloud-storage lag, you only retry the validation, saving significant time and resources.

### Variable name `retries` instead of `max_attempts`

`retries=3` is self-documenting — "retry 3 times". `max_attempts=4` forces the reader to remember that attempts includes the first one. The internal conversion is handled by the `total_attempts` property, which is the only place in the codebase that does `retries + 1`. Users never see or touch `total_attempts`.

---

## 3. Open Questions and Risks

### Idempotency of Tasks

Retry assumes that running a task twice is safe. For most tasks this holds — `copy` and `download` tasks overwrite their destination, so a second attempt produces the same result. However, tasks that append to a destination, send a notification, or have side effects outside Otter's control are not idempotent. Retrying them could corrupt output or trigger duplicate actions. This is not solved by this design. It is the task implementer's responsibility to ensure their `run()` is safe to retry. This should be documented clearly in the task authoring guide. A future mitigation could be a per-task `idempotent: false` flag that disables retry regardless of the policy to function as a documentation that task is not safe to retry, but that is out of scope here.

### Retry Storms

Many tasks in the same step hit the same external endpoints — FTP servers, GCS, the EBI API. If a server goes down and 20 tasks fail simultaneously, they will all retry at the same interval, and likely re-triggering the same failure. Jitter (±20% random offset on the delay) reduces but does not eliminate this. The deeper issue is that Otter has no shared circuit-breaker — if one task detects a 503 from a host, other tasks targeting the same host will still try and fail independently before retrying. A solution could be a shared failure registry across workers, but that requires inter-process state which conflicts with Otter's simple multiprocessing model. For now, jitter is the mitigation and this risk should be accepted consciously.

### `validate()` Retry Semantics

Some validation failures are deterministic, meaning wrong schema, missing file, bad checksum, and retrying them wastes time. It should be called out in documentation: only set `validation.retries > 0` if your validator performs network input/output and is affected by transient errors.

### Abort Event and Sibling Tasks

When a task exhausts all retries, `context.abort.set()` fires and the coordinator kills all workers. Any sibling tasks mid-execution are killed without completing their own retry budgets — even if they would have succeeded on their next attempt. This is correct behaviour — a failed task means the step cannot succeed, so there is no use to continue, but it is worth stating explicitly. A future option could be a `fail_fast: false` step-level flag that lets sibling tasks finish their current attempt before the coordinator shuts down, producing a richer failure manifest. Airflow has opportunity to only retry parallel tasks that failed where it's appropriate, but it's out of scope for now.

### No Global Retry Defaults

Every task that wants retry must configure it explicitly. There is no step-level or global `retry:` default. This is good because different tasks have different failure profiles and a global default would apply retry to tasks where it is inappropriate. The tradeoff is verbosity: in a step with 10 HTTP download tasks, each one needs its own `retry:` block. A future improvement could be a step-level `retry_defaults:` that individual tasks can override, similar to how Airflow supports default args on a DAG.
