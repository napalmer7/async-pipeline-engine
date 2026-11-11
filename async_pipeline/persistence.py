import json
from pathlib import Path
from typing import Any, Dict, Optional


class StateJournal:
    """Basic file-based state journal storing pipeline checkpoints."""

    def __init__(self, journal_path: str | Path) -> None:
        self.path = Path(journal_path)
        if not self.path.exists():
            self._write({})

    def _read(self) -> Dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def _write(self, data: Dict[str, Any]) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def save_checkpoint(
        self,
        task_id: str,
        stage_name: str,
        output_state: Dict[str, Any],
        status: str = "COMPLETED"
    ) -> None:
        """Record stage execution state for a task."""
        data = self._read()
        task_record = data.setdefault(task_id, {"stages": {}, "status": "IN_PROGRESS"})
        task_record["stages"][stage_name] = {
            "status": status,
            "output": output_state
        }
        self._write(data)

    def get_task_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Fetch recorded state for a given task ID."""
        data = self._read()
        return data.get(task_id)
