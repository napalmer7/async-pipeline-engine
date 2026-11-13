"""Example multi-stage ETL workflow using retry policies, validation, and metrics."""

import asyncio
from pathlib import Path
from async_pipeline import (
    EventBus,
    MetricsCollector,
    PayloadValidator,
    PipelineRunner,
    PipelineTask,
    RetryPolicy,
    StageRegistry,
    StateJournal,
)


def create_etl_registry() -> StageRegistry:
    registry = StageRegistry()

    @registry.register(
        "extract",
        input_validator=PayloadValidator(required_keys=["source_records"]),
        description="Extract valid dictionaries from raw source list",
    )
    async def extract(payload: dict) -> dict:
        records = [r for r in payload["source_records"] if isinstance(r, dict)]
        return {"extracted": records}

    @registry.register(
        "transform",
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01),
        description="Normalize pricing and compute tax fields",
    )
    async def transform(payload: dict) -> dict:
        transformed = []
        for rec in payload.get("extracted", []):
            price = float(rec.get("price", 0.0))
            transformed.append({
                "sku": rec.get("sku", "UNKNOWN"),
                "price": price,
                "tax": round(price * 0.07, 2),
            })
        return {"transformed": transformed}

    @registry.register("load", description="Produce final batch summary")
    async def load(payload: dict) -> dict:
        rows = payload.get("transformed", [])
        return {
            "batch_total": round(sum(r["price"] + r["tax"] for r in rows), 2),
            "row_count": len(rows),
        }

    return registry


async def run_example(journal_file: Path) -> dict:
    bus = EventBus()
    metrics = MetricsCollector()
    metrics.attach(bus)

    runner = PipelineRunner(
        registry=create_etl_registry(),
        journal=StateJournal(journal_file),
        event_bus=bus,
    )

    task = PipelineTask(
        task_id="etl-batch-001",
        stages=["extract", "transform", "load"],
        initial_payload={
            "source_records": [
                {"sku": "A-100", "price": 19.99},
                {"sku": "B-200", "price": 5.50},
            ]
        },
    )
    final_state = await runner.execute_task(task)
    return {"final_state": final_state, "metrics": metrics.snapshot()}


if __name__ == "__main__":
    output = asyncio.run(run_example(Path("example_journal.json")))
    print(output)
