package com.twilio.raas.sql.rules;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.ComparisonPredicate;
import com.twilio.raas.sql.InListPredicate;

import org.apache.kudu.client.KuduPredicate;
import org.junit.Test;

public class InListConversionTest {

  @Test
  public void noPredicates() throws Exception {
    assertEquals("Should get an empty collection back",
                 Collections.emptyList(),
                 KuduFilterRule.processForInList(Collections.emptyList()));
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
                 KuduFilterRule.processForInList(predicates));
  }

  @Test
  public void oneFieldOredTogetherWithAnotherEqual() throws Exception {
    final CalciteKuduPredicate lowerRange = new ComparisonPredicate(
      0, KuduPredicate.ComparisonOp.GREATER_EQUAL, Integer.valueOf(14));

    final CalciteKuduPredicate topRange = new ComparisonPredicate(
        0, KuduPredicate.ComparisonOp.LESS, Integer.valueOf(25));

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

    predicates.add(scan1);
    predicates.add(scan2);
    predicates.add(scan3);

    final List<Object> predicateValues = new ArrayList<>();
    predicateValues.add(0);
    predicateValues.add(1);
    predicateValues.add(3);

    final List<CalciteKuduPredicate> expectedScan = new ArrayList<>();
    expectedScan.add(lowerRange);
    expectedScan.add(topRange);
    expectedScan.add(new InListPredicate(1, predicateValues));

    assertEquals("Expected a single scan with a single InListPredicate and two comparasions",
        Collections.singletonList(expectedScan),
        KuduFilterRule.processForInList(predicates));
  }

  @Test
  public void multipleInClauses() throws Exception {
    final CalciteKuduPredicate lowerRange = new ComparisonPredicate(
      0, KuduPredicate.ComparisonOp.GREATER_EQUAL, Integer.valueOf(14));

    final CalciteKuduPredicate topRange = new ComparisonPredicate(
        0, KuduPredicate.ComparisonOp.LESS, Integer.valueOf(25));

    final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
    final ArrayList<CalciteKuduPredicate> scan1 = new ArrayList<>();
    scan1.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(0)));
    scan1.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(10)));
    scan1.add(lowerRange);
    scan1.add(topRange);

    final ArrayList<CalciteKuduPredicate> scan2 = new ArrayList<>();
    scan2.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(1)));
    scan2.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(11)));
    scan2.add(lowerRange);
    scan2.add(topRange);

    final ArrayList<CalciteKuduPredicate> scan3 = new ArrayList<>();
    scan3.add(new ComparisonPredicate(1, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(3)));
    scan3.add(new ComparisonPredicate(2, KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(13)));
    scan3.add(lowerRange);
    scan3.add(topRange);

    predicates.add(scan1);
    predicates.add(scan2);
    predicates.add(scan3);

    final List<Object> predicateValues = new ArrayList<>();
    predicateValues.add(0);
    predicateValues.add(1);
    predicateValues.add(3);

    final List<Object> predicateValuesColumnTwo = new ArrayList<>();
    predicateValuesColumnTwo.add(10);
    predicateValuesColumnTwo.add(11);
    predicateValuesColumnTwo.add(13);

    final List<CalciteKuduPredicate> expectedScan = new ArrayList<>();
    expectedScan.add(lowerRange);
    expectedScan.add(topRange);
    expectedScan.add(new InListPredicate(1, predicateValues));
    expectedScan.add(new InListPredicate(2, predicateValuesColumnTwo));

    assertEquals("Expected a single scan with a single InListPredicate and two comparasions",
        Collections.singletonList(expectedScan), KuduFilterRule.processForInList(predicates));
  }
}
