#pragma once

#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <memory>
#include <string>

class HeadServerMetrics {
public:
    HeadServerMetrics(const std::string& bind_address = "0.0.0.0:9095")
        : registry_(std::make_shared<prometheus::Registry>()),
          exposer_(std::make_unique<prometheus::Exposer>(bind_address)),
          // File operations
          files_uploaded_(prometheus::BuildCounter()
              .Name("head_files_uploaded_total")
              .Help("Total number of files uploaded")
              .Register(*registry_).Add({})),
          files_downloaded_(prometheus::BuildCounter()
              .Name("head_files_downloaded_total")
              .Help("Total number of files downloaded")
              .Register(*registry_).Add({})),
          chunks_created_(prometheus::BuildCounter()
              .Name("head_chunks_created_total")
              .Help("Total number of chunks created from file splits")
              .Register(*registry_).Add({})),
          chunks_transferred_(prometheus::BuildCounter()
              .Name("head_chunks_transferred_total")
              .Help("Total chunks transferred to cluster servers")
              .Register(*registry_).Add({})),
          bytes_ingested_(prometheus::BuildCounter()
              .Name("head_bytes_ingested_total")
              .Help("Total bytes ingested from file uploads")
              .Register(*registry_).Add({})),
          // Read repair
          read_repairs_(prometheus::BuildCounter()
              .Name("head_read_repairs_total")
              .Help("Total number of read repairs performed")
              .Register(*registry_).Add({})),
          corruption_detected_(prometheus::BuildCounter()
              .Name("head_corruption_detected_total")
              .Help("Total number of data corruptions detected via hash voting")
              .Register(*registry_).Add({})),
          // Errors
          errors_(prometheus::BuildCounter()
              .Name("head_errors_total")
              .Help("Total errors during file operations")
              .Register(*registry_)),
          // Cluster health gauges
          healthy_cluster_servers_(prometheus::BuildGauge()
              .Name("head_healthy_cluster_servers")
              .Help("Number of currently healthy cluster servers")
              .Register(*registry_).Add({})),
          // Transfer latency
          chunk_transfer_duration_(prometheus::BuildHistogram()
              .Name("head_chunk_transfer_duration_seconds")
              .Help("Time taken to transfer chunks to cluster servers")
              .Register(*registry_)
              .Add({}, prometheus::Histogram::BucketBoundaries{
                  0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0}))
    {
        exposer_->RegisterCollectable(registry_);
    }

    void record_file_upload(size_t bytes, int chunks) {
        files_uploaded_.Increment();
        bytes_ingested_.Increment(static_cast<double>(bytes));
        chunks_created_.Increment(static_cast<double>(chunks));
    }

    void record_file_download() { files_downloaded_.Increment(); }
    void record_chunk_transfer(double duration_s) {
        chunks_transferred_.Increment();
        chunk_transfer_duration_.Observe(duration_s);
    }
    void record_read_repair() { read_repairs_.Increment(); }
    void record_corruption() { corruption_detected_.Increment(); }
    void record_error(const std::string& type) {
        errors_.Add({{"type", type}}).Increment();
    }
    void set_healthy_servers(int count) {
        healthy_cluster_servers_.Set(static_cast<double>(count));
    }

private:
    std::shared_ptr<prometheus::Registry> registry_;
    std::unique_ptr<prometheus::Exposer> exposer_;

    prometheus::Counter& files_uploaded_;
    prometheus::Counter& files_downloaded_;
    prometheus::Counter& chunks_created_;
    prometheus::Counter& chunks_transferred_;
    prometheus::Counter& bytes_ingested_;
    prometheus::Counter& read_repairs_;
    prometheus::Counter& corruption_detected_;
    prometheus::Family<prometheus::Counter>& errors_;
    prometheus::Gauge& healthy_cluster_servers_;
    prometheus::Histogram& chunk_transfer_duration_;
};
