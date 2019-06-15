package com.dzyun.matches.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RowEntity {

  //rowkey
  private String rowKey;
  //数据
  private String col;

  private String value;
}
