from opentelemetry.trace import SpanKind
from opentelemetry.sdk.trace import SpanProcessor
from threading import Lock


class LiveSpanProcessor(SpanProcessor):
    def __init__(self):
        self.spans = {}
        self.lock = Lock()

    def on_start(self, span, parent_context):
        if span.kind == SpanKind.PRODUCER:
            return
        with self.lock:
            self.spans[span.context.span_id] = {
                "span": span,
                "start_time": span.start_time,
                "status": "in_progress",
                "parent_span_id": getattr(span.parent, "span_id", None),
                "events": [],
            }

    def on_end(self, span):
        with self.lock:
            if span.context.span_id in self.spans:
                self.spans[span.context.span_id]["status"] = "ended"
                self.spans[span.context.span_id]["end_time"] = span.end_time

    def get_snapshot(self):
        with self.lock:
            return self.spans.copy()
