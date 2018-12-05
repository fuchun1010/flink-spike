package com.tank;

import com.tank.stream.ItemStream;
import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink stream demo
 *
 * @author fuchun
 */
public class Stream {
  public static void main(String[] args) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.addSource(new ItemStream()).print();

    try {
      env.execute("stream job");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
