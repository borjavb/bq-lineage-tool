package com.borjav.data.utils;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.TableCell;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class UtilsParser {

  public static String readFile(File path, Charset encoding)
      throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path.toURI()));
    return new String(encoded, encoding);
  }

  public static String readFile(String path, Charset encoding)
      throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, encoding);
  }

  public static String removeComments(String sql) {
    String pattern = "-{2,}.*";
    return sql.replaceAll(pattern, "");
  }


  public static String getStringValue(TableCell cell) {
    return Data.isNull(cell.getV()) ? null : cell.getV().toString();
  }

  public static File createTempDirectory()
      throws IOException {
    final File temp;

    temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

    if (!(temp.delete())) {
      throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
    }

    if (!(temp.mkdir())) {
      throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
    }

    return (temp);
  }

  public static String getLiteral(ResolvedNodes.ResolvedLiteral node) {
    try {
      switch (node.getType().getKind()) {
        case TYPE_STRING:
          return node.getValue().getStringValue();
        case TYPE_BYTES:
          return node.getValue().getBytesValue().toStringUtf8();
        case TYPE_INT64:
          return String.valueOf(node.getValue().getInt64Value());
        case TYPE_FLOAT:
          return String.valueOf(node.getValue().getFloatValue());
        case TYPE_DOUBLE:
          return String.valueOf(node.getValue().getDoubleValue());
        case TYPE_NUMERIC:
          return node.getValue().getNumericValue().toString();
        case TYPE_BOOL:
          return node.getValue().getBoolValue() ? "true" : "false";
        case TYPE_TIMESTAMP:
          return String.valueOf(node.getValue().getTimestampUnixMicros());
        case TYPE_DATE:
          return String.valueOf(node.getValue().getDateValue());
        case TYPE_TIME:
          return String.valueOf(node.getValue().getTimeValue());
        case TYPE_DATETIME:
          return String.valueOf(node.getValue().getDatetimeValue());
        case TYPE_BIGNUMERIC:
          return String.valueOf(node.getValue().getBigNumericValue().floatValue());
        case TYPE_JSON:
          return node.getValue().getJsonValue();
        default:
          return node.getValue().getProto().toString();
      }
    } catch (Exception e) {
      return node.getValue().getProto().toString();
    }
  }
}
