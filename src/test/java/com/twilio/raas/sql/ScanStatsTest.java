package com.twilio.raas.sql;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

public class ScanStatsTest {
  @Test
  public void emptyScannerMetrics() throws Exception {
    assertEquals("Scan Stats should start with an empty list", Collections.emptyList(),
        new KuduScanStats().getScannerMetricsList());
    assertEquals("Scanner count should be 0 as no scans have happened", 0,
                 new KuduScanStats().getScannerCount());
  }
}
