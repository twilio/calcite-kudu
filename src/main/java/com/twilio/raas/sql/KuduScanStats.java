package com.twilio.raas.sql;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single KuduScanStats object is created per query that is executed and is accessed from all
 * the {@link ScannerCallback}s created for each scanner as well as the {@link KuduEnumerable}
 */
public final class KuduScanStats {

  // rowsReadCount and scannerNextBatchRpcCount are accessed concurrently from ScannerCallback
  // for scans running in parallel
  private AtomicLong rowsScannedCount = new AtomicLong(0L);

  private AtomicLong scannerRpcCount = new AtomicLong(0L);

  private long timeToFirstRowMs = -1L;

  private long totalTimeMs = -1L;

  private final long startTime;

  private long scannerCount = 0L;

  private List<ScannerMetrics> scannerMetricsList = Collections.emptyList();

  public KuduScanStats() {
    this.startTime = System.currentTimeMillis();
  }

  public void incrementRowsScannedCount(final long additionalRows) {
    this.rowsScannedCount.updateAndGet(current -> current + additionalRows);
  }

  public void incrementScannerRpcCount(final long additionalRpcs) {
    this.scannerRpcCount.updateAndGet(current -> current + additionalRpcs);
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

  public long getRowsScannedCount() {
    return this.rowsScannedCount.longValue();
  }

  public long getScannerRpcCount() {
    return this.scannerRpcCount.longValue();
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
