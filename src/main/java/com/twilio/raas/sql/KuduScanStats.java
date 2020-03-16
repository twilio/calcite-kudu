package com.twilio.raas.sql;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single KuduScanStats object is created per query that is executed and is accessed from all
 * the {@link ScannerCallback}s created for each scanner as well as the {@link KuduEnumerable}
 */
public final class KuduScanStats {

  // rowsReadCount and scannerNextBatchRpcCount are accessed concurrently from ScannerCallback
  // for scans running in parallel
  private AtomicLong rowsReadCount = new AtomicLong(0L);

  private AtomicLong scannerNextBatchRpcCount = new AtomicLong(0L);

  private long timeToFirstRowMs = -1L;

  private long totalTimeMs = -1L;

  private final long startTime;

  private long scannerCount;

  private List<ScannerMetrics> scannerMetricsList;

  public KuduScanStats() {
    this.startTime = System.currentTimeMillis();
  }

  public void incrementRowsReadCount(final long additionalRows) {
    this.rowsReadCount.updateAndGet(current -> current + additionalRows);
  }

  public void incrementScannerNextBatchRpcCount(final long additionalRpcs) {
    this.scannerNextBatchRpcCount.updateAndGet(current -> current + additionalRpcs);
  }

  public void setTimeToFirstRowMs() {
    this.timeToFirstRowMs = System.currentTimeMillis() - this.startTime;
  }

  public void setTotalTimeMs() {
    this.totalTimeMs = System.currentTimeMillis() - this.startTime;
  }

  public void setScannerMetricsList(List<ScannerMetrics> scannerMetricsList) {
    this.scannerCount = scannerMetricsList.size();
    this.scannerMetricsList = scannerMetricsList;
  }

  public List<ScannerMetrics> getScannerMetricsList() {
    return scannerMetricsList;
  }

  public long getRowsReadCount() {
    return this.rowsReadCount.longValue();
  }

  public long getScannerNextBatchRpcCount() {
    return this.scannerNextBatchRpcCount.longValue();
  }

  public long getTimeToFirstRowMs() {
    return this.timeToFirstRowMs;
  }

  public long getTotalTimeMs() {
    return this.totalTimeMs;
  }

  public long getScannerCount() {
    return scannerCount;
  }

}
