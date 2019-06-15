package com.dzyun.matches.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MsgEntity implements Serializable {

  private String phone_id;
  private Long create_time;
  private String app_name;
  private String main_call_no;
  private String msg;

  private String the_date;
  private String file_no;
}
