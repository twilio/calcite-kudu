package com.twilio.raas.sql;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Test;

public class InListPredicateTest {

  static long REVERSE_TIMESTAMP_EPOCH = Instant.parse("9999-12-31T00:00:00.000000Z").toEpochMilli();

  @Test
  public void stringTranslation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.STRING).build();
    final InListPredicate predicate =  new InListPredicate(0, Arrays.<Object>asList("string", Integer.valueOf(4), Boolean.TRUE));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema, Arrays.asList("string", "4", "true")),
                 predicate.toPredicate(columnSchema, false));
  }

  @Test
  public void booleanTranslation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.BOOL).build();
    final InListPredicate predicate = new InListPredicate(0,
                                                          Arrays.<Object>asList(Boolean.TRUE));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema, Arrays.asList(Boolean.TRUE)),
        predicate.toPredicate(columnSchema, false));
  }

  @Test
  public void int8Translation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.INT8).build();
    final InListPredicate predicate = new InListPredicate(0, Arrays.<Object>asList(Integer.valueOf(0), Long.valueOf(14), Float.valueOf(18.0f)));

    assertEquals(KuduPredicate.newInListPredicate(
                   columnSchema,
                   Arrays.asList(Byte.valueOf("0"), Byte.valueOf("14"), Byte.valueOf("18"))),
        predicate.toPredicate(columnSchema, false));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
            Arrays.asList(Byte.valueOf("-19"), Byte.valueOf("-15"), Byte.valueOf("-1"))),
        predicate.toPredicate(columnSchema, true));
  }

  @Test
  public void int16Translation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.INT16).build();
    final InListPredicate predicate = new InListPredicate(0,
        Arrays.<Object>asList(Integer.valueOf(0), Long.valueOf(14), Float.valueOf(18.0f)));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
            Arrays.asList(Short.valueOf("0"), Short.valueOf("14"), Short.valueOf("18"))),
        predicate.toPredicate(columnSchema, false));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema,

        Arrays.asList(
          Short.valueOf((short) -19),
          Short.valueOf((short) -15),
          Short.valueOf((short) -1))),
        predicate.toPredicate(columnSchema, true));
  }

  @Test
  public void int32Translation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.INT32).build();
    final InListPredicate predicate = new InListPredicate(0,
        Arrays.<Object>asList(Integer.valueOf(0), Long.valueOf(14), Float.valueOf(18.0f)));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
            Arrays.asList(Integer.valueOf("0"), Integer.valueOf("14"), Integer.valueOf("18"))),
        predicate.toPredicate(columnSchema, false));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema,

        Arrays.asList(
            Integer.valueOf(-19),
            Integer.valueOf(-15),
            Integer.valueOf(-1))),
        predicate.toPredicate(columnSchema, true));
  }

  @Test
  public void int64Translation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.INT64).build();
    final InListPredicate predicate = new InListPredicate(0,
        Arrays.<Object>asList(Integer.valueOf(0), Long.valueOf(14), Float.valueOf(18.0f)));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
            Arrays.asList(Long.valueOf("0"), Long.valueOf("14"), Long.valueOf("18"))),
        predicate.toPredicate(columnSchema, false));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema,

        Arrays.asList(Long.valueOf(-19L),
                      Long.valueOf(-15L),
                      Long.valueOf(-1L))),
        predicate.toPredicate(columnSchema, true));
  }

  @Test
  public void floatTranslation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.FLOAT).build();
    final InListPredicate predicate = new InListPredicate(0,
        Arrays.<Object>asList(Integer.valueOf(0), Long.valueOf(14), Float.valueOf(18.0f)));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
            Arrays.asList(Float.valueOf("0"), Float.valueOf("14"), Float.valueOf("18"))),
        predicate.toPredicate(columnSchema, false));
  }

  @Test
  public void doubleTranslation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.DOUBLE).build();
    final InListPredicate predicate = new InListPredicate(0,
        Arrays.<Object>asList(Integer.valueOf(0), Long.valueOf(14), Double.valueOf(18.0d)));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
            Arrays.asList(Double.valueOf("0"), Double.valueOf("14"), Double.valueOf("18"))),
        predicate.toPredicate(columnSchema, false));
  }

  @Test
  public void decimalTranslation() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.DECIMAL)
      .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().build())
      .build();
    final InListPredicate predicate = new InListPredicate(0,
        Arrays.<Object>asList(BigDecimal.valueOf(0L), BigDecimal.valueOf(14L), BigDecimal.valueOf(18L)));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
                                         Arrays.asList(
                                           BigDecimal.valueOf(0L),
                                           BigDecimal.valueOf(14L),
                                           BigDecimal.valueOf(18L))),
        predicate.toPredicate(columnSchema, false));
  }

  @Test
  public void microsecsTranslationLong() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.UNIXTIME_MICROS).build();
    final Long july11 = Long.valueOf(1594450800000L);
    final Long sept11 = Long.valueOf(1599848188243L);
    final InListPredicate predicate = new InListPredicate(0,
                                                          Arrays.<Object>asList(july11, sept11));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
                                         Arrays.asList(july11, sept11)),
        predicate.toPredicate(columnSchema, false));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema,
                                                  Arrays.asList(REVERSE_TIMESTAMP_EPOCH - july11,
                                                                REVERSE_TIMESTAMP_EPOCH - sept11)),
        predicate.toPredicate(columnSchema, true));
  }

  @Test
  public void microsecsTranslationTimestamp() {
    ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("fielda", Type.UNIXTIME_MICROS).build();
    final Timestamp july11 = new Timestamp(1594450800000L);
    final Timestamp sept11 = new Timestamp(1599848188243L);
    final InListPredicate predicate = new InListPredicate(0, Arrays.<Object>asList(july11, sept11));

    assertEquals(KuduPredicate.newInListPredicate(columnSchema, Arrays.asList(july11.toInstant().toEpochMilli(),
                                                                              sept11.toInstant().toEpochMilli())),
        predicate.toPredicate(columnSchema, false));

    assertEquals(
        KuduPredicate.newInListPredicate(columnSchema,
                                         Arrays.asList(REVERSE_TIMESTAMP_EPOCH - july11.toInstant().toEpochMilli(),
                                                       REVERSE_TIMESTAMP_EPOCH - sept11.toInstant().toEpochMilli())),
        predicate.toPredicate(columnSchema, true));
  }
}
