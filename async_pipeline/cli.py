import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import List, Optional
from .engine import PipelineRunner, PipelineTask
from .hooks import EventBus, MetricsCollector
from .persistence import StateJournal
from .registry import StageRegistry


def build_sample_registry() -> StageRegistry:
    """Create a sample ETL registry for CLI demonstration runs."""
    registry = StageRegistry()

    @registry.register("ingest", description="Normalize raw input records")
    async def ingest(payload: dict) -> dict:
        raw_items = payload.get("items", [])
        cleaned = [str(item).strip().lower() for item in raw_items if str(item).strip()]
        return {"cleaned_items": cleaned, "ingested_count": len(cleaned)}

    @registry.register("enrich", description="Compute token lengths and tags")
    async def enrich(payload: dict) -> dict:
        enriched = [
            {"value": item, "length": len(item)}
            for item in payload.get("cleaned_items", [])
        ]
        return {"enriched_items": enriched}

    @registry.register("summarize", description="Aggregate summary metrics")
    async def summarize(payload: dict) -> dict:
        items = payload.get("enriched_items", [])
        total_len = sum(entry["length"] for entry in items)
        return {
            "summary": {
                "total_records": len(items),
                "total_characters": total_len,
            }
        }

    return registry


async def _run_sample_workflow(journal_path: Path, items: List[str]) -> dict:
    registry = build_sample_registry()
    journal = StateJournal(journal_path)
    bus = EventBus()
    metrics = MetricsCollector()
    metrics.attach(bus)

    runner = PipelineRunner(registry=registry, journal=journal, event_bus=bus)
    task = PipelineTask(
        task_id="cli-etl-job-1",
        stages=["ingest", "enrich", "summarize"],
        initial_payload={"items": items},
    )
    output = await runner.execute_task(task)
    return {"output": output, "metrics": metrics.snapshot()}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="async-pipeline",
        description="CLI runner and journal inspector for async-pipeline-engine.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute a built-in sample workflow")
    run_parser.add_argument(
        "--workflow",
        choices=["etl"],
        default="etl",
        help="Workflow template to execute",
    )
    run_parser.add_argument(
        "--journal",
        type=Path,
        default=Path("pipeline_journal.json"),
        help="Path to state journal file",
    )
    run_parser.add_argument(
        "--items",
        nargs="*",
        default=["Alpha", "Beta", "Gamma"],
        help="Input items for sample ETL workflow",
    )

    inspect_parser = subparsers.add_parser("inspect", help="Inspect a saved state journal")
    inspect_parser.add_argument(
        "--journal",
        type=Path,
        required=True,
        help="Path to state journal file",
    )

    args = parser.parse_args(argv)

    if args.command == "run":
        result = asyncio.run(_run_sample_workflow(args.journal, args.items))
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "inspect":
        if not args.journal.exists():
            print(f"Journal file not found: {args.journal}", file=sys.stderr)
            return 1
        journal = StateJournal(args.journal)
        print(json.dumps(journal.list_tasks(), indent=2))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
