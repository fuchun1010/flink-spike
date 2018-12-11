package com.tank.handler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author fuchun
 */
public class JsonConverter {


  public static <T> T toObj(final String jsonStr, Class<T> clazz) {
    final ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(jsonStr, clazz);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

}
