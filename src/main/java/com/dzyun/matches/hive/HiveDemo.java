package com.dzyun.matches.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveDemo {
  public static void main(String[] args) throws Exception {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    Connection connection = DriverManager.getConnection("jdbc:hive2://10.1.62.176:10000/default", "", "");
    Statement stmt = connection.createStatement();
    String querySQL="show databases";
    ResultSet result = stmt.executeQuery(querySQL);
    while (result.next()) {
      System.out.println(result.getString(0));
    }
  }

}