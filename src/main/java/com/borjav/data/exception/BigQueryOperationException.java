

package com.borjav.data.exception;

import com.borjav.data.model.BigQueryTableEntity;

/**
 * Wrapped RunTime exception thrown from BigQuery operations.
 */
public class BigQueryOperationException extends RuntimeException {


  public BigQueryOperationException(BigQueryTableEntity table, Throwable cause) {
    super(String.format("BigQuery Operation exception for%n%s", table), cause);
  }
}
