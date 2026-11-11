from async_pipeline.persistence import StateJournal


def test_save_and_retrieve_checkpoint(tmp_path):
    journal_file = tmp_path / "test_journal.json"
    journal = StateJournal(journal_file)

    journal.save_checkpoint(
        task_id="task-1",
        stage_name="stage_a",
        output_state={"count": 42},
        status="COMPLETED"
    )

    state = journal.get_task_state("task-1")
    assert state is not None
    assert "stage_a" in state["stages"]
    assert state["stages"]["stage_a"]["output"]["count"] == 42
    assert state["stages"]["stage_a"]["status"] == "COMPLETED"
