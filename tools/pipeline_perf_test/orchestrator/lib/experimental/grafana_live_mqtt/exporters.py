import json
import os
import time
import threading
import paho.mqtt.client as mqtt
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    AggregationTemporality,
)
from opentelemetry.sdk._logs.export import LogExporter, LogExportResult

from opentelemetry.sdk.trace import Span

# Global for updating metric attributes with values from spans
test_case = ""

class MQTTClient:
    def __init__(self, broker_url, broker_port):
        self.client = mqtt.Client()
        self.client.connect(broker_url, broker_port)

    def publish(self, topic: str, payload: str, qos=0, retain=False):
        self.client.publish(topic, payload, qos, retain)

    def disconnect(self):
        self.client.disconnect()


class MQTTSpanExporter(SpanExporter):
    def __init__(self, mqtt_client: MQTTClient, topic: str = "test/state"):
        self._client = mqtt_client
        self._topic = topic
        self.lock = threading.Lock()
        self.state = {"Test Suite": "", "Test Case": "", "Test Step": "", "Status": ""}

    def on_start(self, span: Span):
        global test_case
        with self.lock:
            changed = False
            if span.name.startswith("Run Test Suite:"):
                self.state["Test Suite"] = span.name.split(": ")[1]
                self.state["Status"] = "Building"
                changed = True
            if span.name.startswith("Run Test:"):
                self.state["Test Case"] = span.name.split(": ")[1]
                test_case = span.name.split(": ")[1]
                self.state["Status"] = "Running"
                changed = True
            if span.name.startswith("Run Test Step:"):
                self.state["Test Step"] = span.name.split(": ")[1]
                changed = True
            if changed:
                self._client.publish(self._topic, json.dumps(self.state))

    def export(self, spans):
        for span in spans:
            if span.name.startswith("Run Test Suite:"):
                if span.status.is_ok:
                    self.state["Status"] = "Finished"
                else:
                    self.state["Status"] = "Crashed"
                self._client.publish(self._topic, json.dumps(self.state))
        return SpanExportResult.SUCCESS

    def shutdown(self):
        if self.state["Status"] != "Finished":
            self.state["Status"] = "Crashed"
            self._client.publish(self._topic, json.dumps(self.state))

        self._client.disconnect()

    def serialize_span(self, span):
        # Simple JSON serialization example
        return json.dumps(
            {
                "name": span.name,
                "context": {
                    "trace_id": span.context.trace_id,
                    "span_id": span.context.span_id,
                },
                "start_time": span.start_time,
                "end_time": span.end_time,
                "attributes": dict(span.attributes),
            }
        )


class MQTTMetricExporter(MetricExporter):
    def __init__(
        self,
        mqtt_client: MQTTClient,
        topic: str = "otel/metrics",
        preferred_temporality: dict[type, AggregationTemporality] | None = None,
        preferred_aggregation: (
            dict[type, "opentelemetry.sdk.metrics.view.Aggregation"] | None
        ) = None,
    ):
        super().__init__(preferred_temporality, preferred_aggregation)
        self._client = mqtt_client
        self._topic = topic
        self._previous_values = {}
        self._lock = threading.Lock()

        self._gauge_metrics = {
            "process.cpu.usage",
            "container.cpu.usage",
            "process.memory.usage",
            "container.memory.usage",
            "sent",
            "received_logs",
            "logs",
            "logs_produced",
        }

        self._rate_metrics = {
            "sent",
            "logs",
            "logs_produced",
            "container.network.rx",
            "container.network.tx",
        }
        self._buffer = {}

        # Start background publishing thread
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._publish_loop, daemon=True)
        self._thread.start()

    def export(self, metrics_data, timeout_millis: float = 10_000, **kwargs):
        global test_case
        for resource_metrics in metrics_data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    name = metric.name
                    data = metric.data

                    if (
                        name not in self._gauge_metrics
                        and name not in self._rate_metrics
                    ):
                        continue

                    for dp in data.data_points:
                        value = dp.value
                        ts = dp.time_unix_nano / 1e9
                        attrs = dict(dp.attributes)
                        key = (name, frozenset(attrs.items()))
                        component = attrs.get("component_name", "")
                        core_id = attrs.get("core_id", "")

                        full_key = f"{component}: {name}"
                        if component == "backend-service":
                            if name == "logs":
                                full_key = f"{component}: logs.received.total"
                            if name == "otlp_signal_throughput":
                                full_key = f"{component}: logs.received.rate"
                        if component == "load-generator":
                            if name == "logs":
                                full_key = f"{component}: logs.sent.total"
                            if name == "logs_produced":
                                full_key = f"{component}: logs.sent.total"
                            if name == "otlp_signal_throughput":
                                full_key = f"{component}: logs.sent.rate"
                        if component == "syslog-generator":
                            if name == "logs":
                                full_key = "load-generator: logs.sent.total"
                            if name == "sent":
                                full_key = "load-generator: logs.sent.total"
                            if name == "sent_rate":
                                full_key = "load-generator: logs.sent.rate"
                            if name == "logs_produced":
                                full_key = "load-generator: logs.sent.total"
                            if name == "otlp_signal_throughput":
                                full_key = "load-generator: logs.sent.rate"

                        if core_id:
                            full_key = full_key.replace(f"{component}", f"{component} (Core# {core_id})")
                        
                        if component in ['rust-engine', 'go-collector']:
                            full_key = full_key.replace(f"{component}: ", f"{test_case}: ")

                        with self._lock:
                            if name in self._gauge_metrics:
                                self._buffer[full_key] = value

                            if name in self._rate_metrics:
                                prev = self._previous_values.get(key)
                                self._previous_values[key] = (value, ts)

                                if prev:
                                    prev_value, prev_ts = prev
                                    time_delta = ts - prev_ts

                                    if time_delta > 0 and value >= prev_value:
                                        rate = (value - prev_value) / time_delta
                                        if "network" in name:
                                            # Convert bytes/sec to bits/sec
                                            self._buffer[
                                                f"{full_key}_rate"
                                            ] = (8 * rate)
                                        else:
                                            self._buffer[
                                                f"{full_key}_rate"
                                            ] = rate

        return MetricExportResult.SUCCESS

    def _publish_loop(self):
        while not self._stop_event.is_set():
            time.sleep(0.25)
            with self._lock:
                if self._buffer:
                    payload = json.dumps(self._buffer)
                    self._client.publish(self._topic, payload)
                    self._buffer.clear()

    def shutdown(self, timeout_millis: float = 30_000, **kwargs):
        self._client.disconnect()

    def force_flush(self, timeout_millis: float = 30_000):
        """
        No-op flush operation.

        Args:
            timeout_millis (float): Timeout for the flush operation in milliseconds.
        """


class MQTTLogExporter(LogExporter):
    def __init__(self, mqtt_client: MQTTClient, topic: str = "otel/logs"):
        self._client = mqtt_client
        self._topic = topic

    def export(self, batch):
        for log_record in batch:
            payload = self.serialize_log(log_record)
            self._client.publish(self._topic, payload)
        return LogExportResult.SUCCESS

    def shutdown(self):
        self._client.disconnect()

    def serialize_log(self, log_record):
        return json.dumps(
            {
                "timestamp": log_record.timestamp,
                "body": str(log_record.body),
                "severity": log_record.severity_text,
                "attributes": log_record.attributes,
            }
        )
