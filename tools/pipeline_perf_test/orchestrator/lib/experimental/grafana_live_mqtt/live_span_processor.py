from opentelemetry.trace import SpanKind
from opentelemetry.sdk.trace import SpanProcessor
from .exporters import MQTTSpanExporter


class LiveSpanProcessor(SpanProcessor):
    def __init__(self, span_exporter: MQTTSpanExporter):
        self.span_exporter = span_exporter

    def on_start(self, span, parent_context):
        if span.kind == SpanKind.PRODUCER:
            return
        self.span_exporter.on_start(span)

    def on_end(self, span):
        self.span_exporter.export((span,))


# class SimpleSpanProcessor(SpanProcessor):
#     """Simple SpanProcessor implementation.

#     SimpleSpanProcessor is an implementation of `SpanProcessor` that
#     passes ended spans directly to the configured `SpanExporter`.
#     """

#     def __init__(self, span_exporter: SpanExporter):
#         self.span_exporter = span_exporter

#     def on_start(
#         self, span: Span, parent_context: typing.Optional[Context] = None
#     ) -> None:
#         pass

#     def on_end(self, span: ReadableSpan) -> None:
#         if not span.context.trace_flags.sampled:
#             return
#         token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
#         try:
#             self.span_exporter.export((span,))
#         # pylint: disable=broad-exception-caught
#         except Exception:
#             logger.exception("Exception while exporting Span.")
#         detach(token)

#     def shutdown(self) -> None:
#         self.span_exporter.shutdown()

#     def force_flush(self, timeout_millis: int = 30000) -> bool:
#         # pylint: disable=unused-argument
#         return True
