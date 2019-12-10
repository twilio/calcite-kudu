package com.twilio.raas.sql;

import java.util.concurrent.atomic.AtomicLong;

public final class KuduScanStats {
  private long numScanners = 0;

  private AtomicLong rowCount = new AtomicLong(0L);

  private AtomicLong rpcCount = new AtomicLong(0L);

  private AtomicLong msToFirstRow = new AtomicLong(-1L);

  private AtomicLong msTotalTime = new AtomicLong(-1L);

  private final long startTime;

  public KuduScanStats() {
    this.startTime = System.currentTimeMillis();
  }

  public void setNumScanners(final long numScanners) {
    this.numScanners = numScanners;
  }

  public void incrementRowCount(final long additionalRows) {
    this.rowCount.updateAndGet(current -> current + additionalRows);
  }

  public void incrementRpcCount(final long additionalRpcs) {
    this.rpcCount.updateAndGet(current -> current + additionalRpcs);
  }

  public void setFirstRow() {
    this.msToFirstRow.compareAndSet(-1L, System.currentTimeMillis() - this.startTime);
  }

  public void setTotalTime() {
    this.msTotalTime.compareAndSet(-1L, System.currentTimeMillis() - this.startTime);
  }

  public long getNumScanners() {
    return this.numScanners;
  }

  public long getRowCount() {
    return this.rowCount.longValue();
  }

  public long getRpcCount() {
    return this.rpcCount.longValue();
  }

  public long getMsToFirstRow() {
    return this.msToFirstRow.longValue();
  }

  public long getMsTotalTime() {
    return this.msTotalTime.longValue();
  }
}
