#pragma once

#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <memory>
#include <string>

class MetricsExporter {
public:
    MetricsExporter(const std::string& bind_address = "0.0.0.0:9091");
    ~MetricsExporter() = default;

    // Update metrics
    void update_connections(int count);
    void update_system_metrics(float cpu, float ram, float disk);
    void record_message(size_t bytes, double processing_time_ns);
    void record_error(const std::string& type);

private:
    // Prometheus metrics
    std::shared_ptr<prometheus::Registry> registry_;
    std::unique_ptr<prometheus::Exposer> exposer_;
    
    // Gauges
    prometheus::Gauge& active_connections_;
    prometheus::Gauge& cpu_usage_;
    prometheus::Gauge& ram_usage_;
    prometheus::Gauge& disk_usage_;
    
    // Counters
    prometheus::Counter& messages_received_;
    prometheus::Counter& bytes_received_;
    prometheus::Family<prometheus::Counter>& errors_total_;
    prometheus::Counter& errors_total_unknown_;
    
    // Histograms
    prometheus::Histogram& processing_time_histogram_;
    
    // Helper to create labels
    using Labels = std::map<std::string, std::string>;
    static const Labels DEFAULT_LABELS;
};
