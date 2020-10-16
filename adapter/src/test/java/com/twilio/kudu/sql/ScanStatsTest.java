/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

public class ScanStatsTest {
  @Test
  public void emptyScannerMetrics() throws Exception {
    assertEquals("Scan Stats should start with an empty list", Collections.emptyList(),
        new KuduScanStats().getScannerMetricsList());
    assertEquals("Scanner count should be 0 as no scans have happened", 0, new KuduScanStats().getScannerCount());
  }
}
