package com.dzyun.matches.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RowEntity implements Serializable {

  private String rowKey;
  private String col;
  private String value;
}
