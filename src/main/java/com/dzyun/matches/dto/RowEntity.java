package com.dzyun.matches.dto;

import java.io.Serializable;


public class RowEntity implements Serializable {

  private String rowKey;
  private String value;

  public String getRowKey() {
    return rowKey;
  }

  public void setRowKey(String rowKey) {
    this.rowKey = rowKey;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public RowEntity() {
  }

  public RowEntity(String rowKey, String value) {
    this.rowKey = rowKey;
    this.value = value;
  }
}
