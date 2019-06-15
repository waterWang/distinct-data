package com.dzyun.matches.dto;

import java.io.Serializable;


public class MsgEntity implements Serializable {

  private String phone_id;
  private Long create_time;
  private String app_name;
  private String main_call_no;
  private String msg;

  private String the_date;
  private String file_no;

  public String getPhone_id() {
    return phone_id;
  }

  public void setPhone_id(String phone_id) {
    this.phone_id = phone_id;
  }

  public Long getCreate_time() {
    return create_time;
  }

  public void setCreate_time(Long create_time) {
    this.create_time = create_time;
  }

  public String getApp_name() {
    return app_name;
  }

  public void setApp_name(String app_name) {
    this.app_name = app_name;
  }

  public String getMain_call_no() {
    return main_call_no;
  }



  public void setMain_call_no(String main_call_no) {
    this.main_call_no = main_call_no;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public String getThe_date() {
    return the_date;
  }

  public void setThe_date(String the_date) {
    this.the_date = the_date;
  }

  public String getFile_no() {
    return file_no;
  }

  public void setFile_no(String file_no) {
    this.file_no = file_no;
  }

  public MsgEntity() {
  }

  public MsgEntity(String phone_id, Long create_time, String app_name, String main_call_no,
      String msg, String the_date, String file_no) {
    this.phone_id = phone_id;
    this.create_time = create_time;
    this.app_name = app_name;
    this.main_call_no = main_call_no;
    this.msg = msg;
    this.the_date = the_date;
    this.file_no = file_no;
  }
}
