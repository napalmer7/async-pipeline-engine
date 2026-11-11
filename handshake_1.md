# Title:
Implement Durable State-Checkpointing and Priority Weighting in the Async Pipeline Engine

## Task Description:
Overview
The async_pipeline repository provides a lightweight Python library for executing asynchronous background workflows and data transformation pipelines. Currently, the engine handles concurrency well via asyncio, but lacks durable state-checkpointing and priority-based scheduling. If a worker crashes or is cancelled mid-pipeline, intermediate progress is lost, and all jobs are treated with equal weight.

Requirements
Durable State-Checkpointing (persistence.py / engine.py):

Extend the persistence layer to checkpoint completed stages asynchronously using an SQLite-backed or JSON-backed journal.

Ensure that when a pipeline restarts or resumes after interruption, it skips already-completed stages and resumes from the exact point of failure without duplicating data or side effects.

Priority-Weighted Worker Queue (engine.py):

Update the task router's internal queue to support numerical priority weights (e.g., high-priority tasks executing ahead of standard tasks).

Implement proper handling for concurrent task cancellation so that cancelling a parent pipeline safely propagates cleanup signals to active child stages.

Testing & Verification:

Add comprehensive test cases in tests/test_persistence.py and tests/test_engine.py covering crash recovery, state resumption, priority ordering, and concurrent cancellation edge cases.

Ensure all tests pass cleanly via pytest.






## First Message:
I want to add new functionality to my async pipeline engine in this repository so that it can support some standard features. These features are durable state-checkpointing and priority-weighted scheduling.

We need to review the current codebase, particularly the engine.py and persistence.py files, along with the included test suite. Analyze how stages are executed, tracked then determine how to add SQLite-backed checkpointing to build out a design proposal for the changes.

## SHow to Judge:
- **Functionality & Resumption:** Does the engine inspect previous checkpoints before running a stage? If a 3-stage pipeline fails at stage 2, does a subsequent run skip stage 1 and pick up directly at stage 2?
- **Concurrency & Priority Ordering:** Does the internal queue prioritize tasks by their integer weight? Does `asyncio.PriorityQueue` or equivalent maintain heap order without blocking other tasks?
- **Crash Safety:** Are SQLite operations atomic and safe against concurrent writes from multiple worker coroutines?
- **Testing:** Are tests comprehensive, deterministic, and passing via `pytest` without network access or timeouts?
- **Code Quality:** Are type hints intact, exceptions cleanly handled via custom hierarchy (`PipelineError`), and zero external dependencies introduced beyond the standard library and pytest?