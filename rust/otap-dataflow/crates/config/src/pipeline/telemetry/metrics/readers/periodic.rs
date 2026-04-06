// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Periodic reader level configurations.

pub mod otlp;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::pipeline::telemetry::metrics::readers::periodic::otlp::OtlpExporterConfig;

/// OpenTelemetry Metrics Periodic Exporter configuration.
/// 
/// Variants:
/// - `console`: Writes metrics to the console.
/// - `otlp`: Sends metrics using the OpenTelemetry Protocol.
/// 
/// Note: Variant doc comments are intentionally omitted because they produce
/// inconsistent `description` metadata across the JSON Schema `oneOf`
/// subschemas, which is rejected by strict schema validators that require
/// identical property definitions across subschema variants
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "config", rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum MetricsPeriodicExporterConfig {
    Console,
    Otlp(OtlpExporterConfig),
}

#[cfg(test)]
mod tests {
    use crate::pipeline::telemetry::metrics::readers::{Temporality, periodic::otlp::OtlpProtocol};

    use super::*;

    #[test]
    fn test_metrics_periodic_exporter_config_deserialize_console() {
        let yaml_str = r#"
            type: console
            "#;

        let config: MetricsPeriodicExporterConfig = serde_yaml::from_str(yaml_str).unwrap();

        assert_eq!(config, MetricsPeriodicExporterConfig::Console);
    }

    #[test]
    fn test_metrics_periodic_exporter_config_deserialize_otlp() {
        let yaml_str = r#"
            type: otlp
            config:
                endpoint: "http://localhost:4317"
                protocol: "grpc/protobuf"
        "#;
        let config: MetricsPeriodicExporterConfig = serde_yaml::from_str(yaml_str).unwrap();
        assert_eq!(
            config,
            MetricsPeriodicExporterConfig::Otlp(OtlpExporterConfig {
                endpoint: "http://localhost:4317".to_string(),
                protocol: OtlpProtocol::Grpc,
                temporality: Temporality::Cumulative,
                tls: None,
            })
        );
    }
}
