import pytest

from sqo_orchestrator.core import NodeDB, TelemetryReplicator


class FlakySink:
    def __init__(self):
        self.calls = 0
        self.events = []

    def append(self, event):
        self.events.append(event)

    def flush(self):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated sink failure")


def test_cursor_is_not_advanced_before_durable_sink_flush(tmp_path):
    db = NodeDB(tmp_path / "db.sqlite", "node-1")
    db.submit_many([])
    with db.connect() as c:
        c.execute("BEGIN IMMEDIATE")
        for i in range(3):
            db._event(c, "job.test", f"j{i}", {"i": i})
        c.commit()

    sink = FlakySink()
    rep = TelemetryReplicator(db, sink)
    with pytest.raises(RuntimeError):
        rep.flush()
    assert rep.cursor == 0
    assert db.get_meta("telemetry.cursor") in (None, "0")

    assert rep.flush() == 3
    assert rep.cursor == 3
