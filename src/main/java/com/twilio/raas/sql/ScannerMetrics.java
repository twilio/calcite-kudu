package com.twilio.raas.sql;

import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RemoteTablet;
import org.apache.kudu.client.ResourceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Metric information for a single kudu scanner
 */
public class ScannerMetrics {

  public final String table;
  public final String tablet;
  public final String scannerId;
  public final String startPrimaryKey;
  public final String endPrimaryKey;
  public final Map<String, Long> metrics;

  private static final Logger logger = LoggerFactory.getLogger(ScannerMetrics.class);

  public ScannerMetrics(AsyncKuduScanner scanner) {
    KuduTable table  = (KuduTable)getField(scanner, "table", KuduTable.class);
    RemoteTablet tablet  = (RemoteTablet)getField(scanner, "tablet", RemoteTablet.class);
    byte[] scannerId  = (byte[])getField(scanner, "scannerId", byte[].class);
    byte[] startPrimaryKey  = (byte[])getField(scanner, "startPrimaryKey", byte[].class);
    byte[] endPrimaryKey  = (byte[])getField(scanner, "endPrimaryKey", byte[].class);
    if (table != null) {
        this.table = table.getName();
    }
    else {
        this.table = "UNKNOWN_TABLE";
    }
    this.tablet = tablet !=null ? tablet.toString() : "null";
    this.scannerId = Bytes.pretty(scannerId);
    this.startPrimaryKey = Bytes.pretty(startPrimaryKey);
    this.endPrimaryKey = Bytes.pretty(endPrimaryKey);
    ResourceMetrics resourceMetrics = scanner.getResourceMetrics();
    this.metrics = resourceMetrics != null ? resourceMetrics.get() : new HashMap<>();
  }

  /**
   * @return the object for the specified fieldName if the field exists in AsyncKuduScanner and
   * if the type of the object matches expectedClass
   */
  private Object getField(AsyncKuduScanner scanner, String fieldName, Class<?> expectedClass) {
    try {
      Field field = AsyncKuduScanner.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      Object val = field.get(scanner);
      return  (expectedClass.isInstance(val)) ? val : null;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      logger.error("AsyncKuduScanner does not have field " + fieldName);
      return null;
    }
  }

  @Override
  public String toString() {
    return "ScanMetrics{" +
      "table='" + table + '\'' +
      ", tablet='" + tablet + '\'' +
      ", scannerId='" + scannerId + '\'' +
      ", startPrimaryKey='" + startPrimaryKey + '\'' +
      ", endPrimaryKey='" + endPrimaryKey + '\'' +
      ", metrics=" + metrics +
      '}';
  }
}
