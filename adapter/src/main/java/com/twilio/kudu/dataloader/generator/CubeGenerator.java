/* Copyright 2021 Twilio, Inc
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
package com.twilio.kudu.dataloader.generator;

import com.twilio.kudu.dataloader.DataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Generate rows using the row counts of a cube grouped by its primary keys.
 * This is useful to generate data of a non-uniform distribution so that the
 * rollup of fact rows in a cube can be modelled.
 */
public class CubeGenerator extends MultipleColumnValueGenerator {

  // columns that used in the GROUP BY clause of the materialized view
  public List<String> groupedColumns;
  // resource that contains counts grouped by the above columns
  public String groupedColumnCountsResource;
  // number of unique groups used to generate data for
  public int numUniqueGroups;

  private int index = -1;
  private Random rand = new Random();
  // map from cumulative count to the cube pk grouped column values
  // When we need to generate column values the chance that a particular row is
  // choses depends on
  // its count so we can model a non-uniform distribution
  private TreeMap<Integer, Object[]> groupedColumnCountMap = new TreeMap<>();
  // while generating column values we pick a random int from [1,
  // totalCumulativeCount] and chose
  // the row that maps to this number
  private int totalCumulativeCount = 0;

  // map from column name to single column value generator (from Scenario)
  private Map<String, SingleColumnValueGenerator> columnNameToValueGenerator;

  private static final Logger logger = LoggerFactory.getLogger(CubeGenerator.class);

  @Override
  public List<String> getColumnNames() {
    return groupedColumns;
  }

  @Override
  public void reset() {
    index = 1 + rand.nextInt(totalCumulativeCount);
  }

  @Override
  public Object getColumnValue(String columnName) {
    int colIndex = groupedColumns.indexOf(columnName);
    if (colIndex == -1) {
      throw new RuntimeException("Column not found : " + columnName);
    }
    Map.Entry<Integer, Object[]> entry = groupedColumnCountMap.ceilingEntry(index);
    if (entry != null) {
      return entry.getValue()[colIndex];
    } else {
      throw new RuntimeException("Index not found : " + index + " total count : " + totalCumulativeCount + " "
          + "min : " + groupedColumnCountMap.firstKey() + " max: " + groupedColumnCountMap.lastKey());
    }
  }

  @Override
  public void initialize() {
    logger.info("Reading from {}", groupedColumnCountsResource);
    final InputStream fileStream = this.getClass().getResourceAsStream(groupedColumnCountsResource);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream));
    try {
      int numGroupedCounts = Integer.valueOf(reader.readLine());
      int[] groupedCounts = new int[numGroupedCounts];
      int index = 0;
      while (reader.ready()) {
        String line = reader.readLine();
        groupedCounts[index++] = Integer.valueOf(line.substring(1, line.length() - 1));
      }
      Collections.shuffle(Arrays.asList(groupedCounts));
      int groupedCountIndex;
      for (int i = 0; i < numUniqueGroups; ++i) {
        // handle the case when we want to generate rows for more unique groups than we
        // have
        // counts for
        groupedCountIndex = i % numGroupedCounts;
        totalCumulativeCount += groupedCounts[groupedCountIndex];
        Object[] columnValues = new Object[groupedColumns.size()];
        for (int j = 0; j < groupedColumns.size(); ++j) {
          columnValues[j] = columnNameToValueGenerator.get(groupedColumns.get(j)).getColumnValue();
        }
        groupedColumnCountMap.put(totalCumulativeCount, columnValues);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setColumnNameToValueGenerator(Map<String, SingleColumnValueGenerator> columnNameToValueGenerator) {
    this.columnNameToValueGenerator = columnNameToValueGenerator;
  }
}
