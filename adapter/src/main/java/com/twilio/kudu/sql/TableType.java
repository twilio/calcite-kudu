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

public enum TableType {
  FACT(1e12d), CUBE(1e9d), DIMENSION(1000.0), SYSTEM(1000.0);

  private double estimatedRowCount;

  TableType(final double estimatedRowCount) {
    this.estimatedRowCount = Double.valueOf(estimatedRowCount);
  }

  /**
   * Returns the estimated row count for this table type.
   *
   * @return estimated row count
   */
  public Double getRowCount() {
    return this.estimatedRowCount;
  }
}
