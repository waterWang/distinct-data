package com.dzyun.matches.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.lang.StringUtils;

public class ShaUtils {

  public static String evaluate(final String str) {
    if (StringUtils.isBlank(str)) {
//      logger.error("----------------------msg加密出错，rowkey");
      return null;
    }
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
    digest.update(str.getBytes());
    byte messageDigest[] = digest.digest();
    StringBuffer hexString = new StringBuffer();
    for (int i = 0; i < messageDigest.length; i++) {
      String shaHex = Integer.toHexString(messageDigest[i] & 0xff);
      if (shaHex.length() < 2) {
        hexString.append(0);
      }
      hexString.append(shaHex);
    }
    return hexString.toString();
  }

  private static String strFormat(String s1, String s2, String s3, String s4) {
    StringBuilder sb = new StringBuilder();
    sb.append(s1);
    sb.append(s2);
    sb.append(s3);
    sb.append(s4);
    return sb.toString();
  }

  public static String encrypt(String[] arr) {
    //phoneNo,createTime,mailCallNo,msg
    return evaluate(strFormat(arr[0], arr[1], arr[3], arr[4]));
  }


  public static void main(String[] args) {
    String s = "8618328822766\t1560008781\t江西农信\t96268\t【江西农信】您尾号水电费水电费所发生的水电费水电费水电费水电费1";
    System.out.println(encrypt(s.split("\t")));
  }

}
