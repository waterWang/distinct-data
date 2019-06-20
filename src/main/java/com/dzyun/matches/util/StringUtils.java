package com.dzyun.matches.util;

import java.io.Serializable;
import java.text.ParseException;

public class StringUtils extends org.apache.commons.lang3.StringUtils implements Serializable {


  public static String fileName2TheDate(String fileName) {
    String date = fileName.split("_")[0].substring(2);
    String theDate = null;
    try {
      theDate = DateUtils.strToDateFormat(date);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return theDate;
  }

}
