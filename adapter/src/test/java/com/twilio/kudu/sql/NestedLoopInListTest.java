package com.twilio.kudu.sql;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kudu.client.KuduPredicate;
import org.junit.Test;

public class NestedLoopInListTest {

  @Test
  public void noPredicates() throws Exception {
    assertEquals("Should get an empty collection back", Collections.emptyList(),
        KuduEnumerable.processForInList(Collections.emptyList()));
  }

  @Test
  public void oneFieldOredTogether() throws Exception {
    final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
    final ArrayList<CalciteKuduPredicate> scan1 = new ArrayList<>();
    scan1.add(new ComparisonPredicate(0, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));

    final ArrayList<CalciteKuduPredicate> scan2 = new ArrayList<>();
    scan2.add(new ComparisonPredicate(0, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(1)));

    final ArrayList<CalciteKuduPredicate> scan3 = new ArrayList<>();
    scan3.add(new ComparisonPredicate(0, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(3)));

    predicates.add(scan1);
    predicates.add(scan2);
    predicates.add(scan3);

    final List<Object> predicateValues = new ArrayList<>();
    predicateValues.add(0);
    predicateValues.add(1);
    predicateValues.add(3);
    assertEquals("Expected a single scan with a single InListPredicate",
        Collections.singletonList(Collections.singletonList(new InListPredicate(0, predicateValues))),
        KuduEnumerable.processForInList(predicates));
  }

  @Test
  public void oneFieldOredTogetherWithAnotherEqual() throws Exception {
    final CalciteKuduPredicate lowerRange = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.GREATER_EQUAL,
        Integer.valueOf(14));

    final CalciteKuduPredicate topRange = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.LESS,
        Integer.valueOf(25));

    final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
    final ArrayList<CalciteKuduPredicate> scan1 = new ArrayList<>();
    scan1.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    scan1.add(lowerRange);
    scan1.add(topRange);

    final ArrayList<CalciteKuduPredicate> scan2 = new ArrayList<>();
    scan2.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(1)));
    scan2.add(lowerRange);
    scan2.add(topRange);

    final ArrayList<CalciteKuduPredicate> scan3 = new ArrayList<>();
    scan3.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(3)));
    scan3.add(lowerRange);
    scan3.add(topRange);

    // ($1 = 0 AND $0 >= 14 AND $0 < 25)
    // OR ($1 = 1 AND $0 >= 14 AND $0 < 25)
    // OR ($1 = 3 AND $0 >= 14 AND $0 < 25)
    predicates.add(scan1);
    predicates.add(scan2);
    predicates.add(scan3);

    // Translate to ($0 >= 14 AND $0 < 25 AND $1 IN (0, 1, 3))
    final List<Object> predicateValues = new ArrayList<>();
    predicateValues.add(0);
    predicateValues.add(1);
    predicateValues.add(3);

    final List<CalciteKuduPredicate> expectedScan = new ArrayList<>();
    expectedScan.add(lowerRange);
    expectedScan.add(topRange);
    expectedScan.add(new InListPredicate(1, predicateValues));

    assertEquals("Expected a single scan with a single InListPredicate and two comparisons",
        Collections.singletonList(expectedScan), KuduEnumerable.processForInList(predicates));
  }

  @Test
  public void multipleInClauses() throws Exception {
    final CalciteKuduPredicate lowerRange = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.GREATER_EQUAL,
        Integer.valueOf(14));

    final CalciteKuduPredicate topRange = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.LESS,
        Integer.valueOf(25));

    // ($1 = 0 AND $2 = 10 AND $0 >= 25 AND $0 < 14)
    final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
    final ArrayList<CalciteKuduPredicate> scan1 = new ArrayList<>();
    scan1.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    scan1.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(10)));
    scan1.add(lowerRange);
    scan1.add(topRange);

    // ($1 = 0 AND $2 = 11 AND $0 >= 25 AND $0 < 14)
    final ArrayList<CalciteKuduPredicate> scan2 = new ArrayList<>();
    scan2.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    scan2.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(11)));
    scan2.add(lowerRange);
    scan2.add(topRange);

    // ($1 = 0 AND $2 = 13 AND $0 >= 25 AND $0 < 14)
    final ArrayList<CalciteKuduPredicate> scan3 = new ArrayList<>();
    scan3.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    scan3.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(13)));
    scan3.add(lowerRange);
    scan3.add(topRange);

    predicates.add(scan1);
    predicates.add(scan2);
    predicates.add(scan3);

    final List<Object> predicateValuesColumnTwo = new ArrayList<>();
    predicateValuesColumnTwo.add(10);
    predicateValuesColumnTwo.add(11);
    predicateValuesColumnTwo.add(13);

    // $1 = 0 AND $0 >= 14 AND $0 < 25 AND $0 < 14 AND $2 IN (10, 11, 13)
    final List<CalciteKuduPredicate> expectedScan = new ArrayList<>();
    expectedScan.add(lowerRange);
    expectedScan.add(topRange);
    expectedScan.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    expectedScan.add(new InListPredicate(2, predicateValuesColumnTwo));

    assertEquals("Expected a single scan with a single InListPredicate and three comparisons",
        Collections.singletonList(expectedScan), KuduEnumerable.processForInList(predicates));
  }

  @Test
  public void multipleEquality() throws Exception {
    final CalciteKuduPredicate lowerRange = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.GREATER_EQUAL,
        Integer.valueOf(14));

    final CalciteKuduPredicate topRange = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.LESS,
        Integer.valueOf(25));

    // ($1 = 0 AND $1 = 100 AND $2 = 10 AND $0 < 25 AND $0 < 14)
    final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
    final ArrayList<CalciteKuduPredicate> scan1 = new ArrayList<>();
    scan1.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    scan1.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(100)));
    scan1.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(10)));
    scan1.add(lowerRange);
    scan1.add(topRange);

    // ($1 = 1 AND $1 = 101 AND $2 = 11 AND $0 < 25 AND $0 < 14)
    final ArrayList<CalciteKuduPredicate> scan2 = new ArrayList<>();
    scan2.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(1)));
    scan2.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(101)));
    scan2.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(11)));
    scan2.add(lowerRange);
    scan2.add(topRange);

    predicates.add(scan1);
    predicates.add(scan2);

    assertEquals("Expected Scan to remain the same", predicates, KuduEnumerable.processForInList(predicates));
  }
}
