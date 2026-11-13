import json
from async_pipeline.cli import main


def test_cli_run_and_inspect(tmp_path, capsys):
    journal_path = tmp_path / "cli_journal.json"

    exit_code = main(
        ["run", "--workflow", "etl", "--journal", str(journal_path), "--items", "One", "Two"]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    run_payload = json.loads(captured.out)
    assert run_payload["output"]["summary"]["total_records"] == 2
    assert run_payload["metrics"]["tasks"]["completed"] == 1

    inspect_code = main(["inspect", "--journal", str(journal_path)])
    assert inspect_code == 0
    inspect_out = json.loads(capsys.readouterr().out)
    assert "cli-etl-job-1" in inspect_out
