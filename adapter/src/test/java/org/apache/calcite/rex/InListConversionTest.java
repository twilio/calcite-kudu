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
package org.apache.calcite.rex;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class InListConversionTest extends RexProgramTestBase {

  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void oneFieldOredTogether() {
    final RexNode aRef = input(tInt(), 0);
    // $0 = 0 OR $0 = 1 OR $0 = 2
    RexNode expr = or(eq(aRef, literal(0)), eq(aRef, literal(1)), eq(aRef, literal(2)));
    final String simplified = "SEARCH($0, Sarg[0, 1, 2])";
    final String expanded = "OR(=($0, 0), =($0, 1), =($0, 2))";
    checkSimplify(expr, simplified).expandedSearch(expanded);
  }

  @Ignore
  public void oneFieldOredTogetherWithAnotherEqual() throws Exception {
    final RexNode aRef = input(tInt(), 0);
    final RexNode bRef = input(tInt(), 1);
    // ($1 = 0 AND $0 >= 14 AND $0 < 25)
    // OR ($1 = 1 AND $0 >= 14 AND $0 < 25)
    // OR ($1 = 3 AND $0 >= 14 AND $0 < 25)
    RexNode expr1 = and(eq(bRef, literal(0)), ge(aRef, literal(14)), lt(aRef, literal(25)));
    RexNode expr2 = and(eq(bRef, literal(1)), ge(aRef, literal(14)), lt(aRef, literal(25)));
    RexNode expr3 = and(eq(bRef, literal(3)), ge(aRef, literal(14)), lt(aRef, literal(25)));
    RexNode expr = or(expr1, expr2, expr3);
    // Translate to ($0 >= 14 AND $0 < 25 AND $1 IN (0, 1, 3))
    final String simplified = "AND(SEARCH($1, Sarg[0, 1, 2]), SEARCH($0, Sarg[[14..25)]))";
    // this currently fails
    checkSimplify(expr, simplified);
  }

  @Ignore
  public void multipleInClauses() throws Exception {
    final RexNode aRef = input(tInt(), 0);
    final RexNode bRef = input(tInt(), 1);
    final RexNode cRef = input(tInt(), 2);
    // ($1 = 0 AND $2 = 10 AND $0 >= 25 AND $0 < 14)
    // OR ($1 = 0 AND $2 = 11 AND $0 >= 25 AND $0 < 14)
    // OR ($1 = 0 AND $2 = 13 AND $0 >= 25 AND $0 < 14)
    RexNode expr1 = and(eq(bRef, literal(0)), eq(cRef, literal(10)), ge(aRef, literal(14)), lt(aRef, literal(25)));
    RexNode expr2 = and(eq(bRef, literal(0)), eq(cRef, literal(11)), ge(aRef, literal(14)), lt(aRef, literal(25)));
    RexNode expr3 = and(eq(bRef, literal(0)), eq(cRef, literal(13)), ge(aRef, literal(14)), lt(aRef, literal(25)));
    RexNode expr = or(expr1, expr2, expr3);

    // $1 = 0 AND $0 >= 14 AND $0 < 25 AND $0 < 14 AND $2 IN (10, 11, 13)
    final String simplified = "AND(SEARCH($2, Sarg[10, 11, 13]), SEARCH($0, Sarg[[14..25)]), " + "SEARCH($1, Sarg[0]))";
    // this currently fails
    checkSimplify(expr, simplified);
  }

  @Ignore
  public void multipleEquality() throws Exception {
    final RexNode aRef = input(tInt(), 0);
    final RexNode bRef = input(tInt(), 1);
    final RexNode cRef = input(tInt(), 2);
    // ($1 = 0 AND $1 = 100 AND $2 = 10 AND $0 < 25 AND $0 < 14)
    // OR ($1 = 1 AND $1 = 101 AND $2 = 11 AND $0 < 25 AND $0 < 14)
    RexNode expr1 = and(eq(bRef, literal(0)), eq(bRef, literal(100)), eq(cRef, literal(10)), ge(aRef, literal(14)),
        lt(aRef, literal(25)));
    RexNode expr2 = and(eq(bRef, literal(1)), eq(bRef, literal(101)), eq(cRef, literal(11)), ge(aRef, literal(14)),
        lt(aRef, literal(25)));
    RexNode expr = or(expr1, expr2);

    final String simplified = "OR(AND(SEARCH($1, Sarg[]), SEARCH($2, Sarg[10]), SEARCH($0, Sarg[[14..25)])), AND(SEARCH($1, Sarg[]), SEARCH($2, Sarg[11]), SEARCH($0, Sarg[[14..25)])))";
    final String expanded = "false";
    // this currently fails
    checkSimplify(expr, simplified).expandedSearch(expanded);
  }
}
