package com.dzyun.matches.util;


import java.io.Serializable;

public class MsgException extends RuntimeException implements Serializable {

  private static final long serialVersionUID = 209248116271894410L;

  public MsgException(String message) {
    super(message);
  }

  public MsgException(Throwable e) {
    super(e);
  }

  public MsgException(String message, Throwable e) {
    super(message, e);
  }
}
