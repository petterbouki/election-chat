"""
observability/tracer.py — Tracing end-to-end des requêtes (Level 4).
Enregistre intent, SQL, latence, tokens pour chaque requête.
"""

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

TRACE_FILE = "data/traces.jsonl"


@dataclass
class Trace:
    trace_id: str
    timestamp: str
    question: str
    intent: Optional[str] = None
    route: Optional[str] = None          # sql / rag / both
    sql_generated: Optional[str] = None
    sql_valid: Optional[bool] = None
    retrieval_count: Optional[int] = None
    rows_returned: Optional[int] = None
    chart_type: Optional[str] = None
    has_error: bool = False
    error_msg: Optional[str] = None
    latency_ms: float = 0.0
    tokens_input: Optional[int] = None
    tokens_output: Optional[int] = None


class Tracer:
    def __init__(self, trace_file: str = TRACE_FILE):
        self.trace_file = Path(trace_file)
        self.trace_file.parent.mkdir(parents=True, exist_ok=True)
        self._current: Optional[Trace] = None
        self._t0: float = 0

    def start(self, question: str) -> Trace:
        import uuid
        self._t0 = time.time()
        self._current = Trace(
            trace_id=str(uuid.uuid4())[:8],
            timestamp=datetime.utcnow().isoformat(),
            question=question,
        )
        return self._current

    def finish(self, trace: Trace):
        trace.latency_ms = (time.time() - self._t0) * 1000
        with open(self.trace_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(trace), ensure_ascii=False) + "\n")
        log.debug(f"Trace {trace.trace_id}: {trace.latency_ms:.0f}ms, intent={trace.intent}")

    def load_traces(self) -> list[dict]:
        if not self.trace_file.exists():
            return []
        traces = []
        with open(self.trace_file) as f:
            for line in f:
                if line.strip():
                    traces.append(json.loads(line))
        return traces
