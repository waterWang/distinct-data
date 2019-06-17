package com.dzyun.matches.util;

import java.io.FileInputStream;
import java.net.URL;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

public class YamlUtil {

  public static String getPatam(String key) {
    return getPatam("../../../../app-prod.yml", key);
  }

  public static String getPatam(String filePath, String key) {
    Map props = null;
    try {
      Yaml yaml = new Yaml();
      URL url = YamlUtil.class.getResource(filePath);
      if (url != null) {
        props = (Map) yaml.load(new FileInputStream(url.getFile()));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return props.get(key).toString();
  }

  public static void main(String[] args) {
    System.out.println(getPatam("quorum"));
  }

}
