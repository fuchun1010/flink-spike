package com.tank;

import com.tank.handler.StreamHandler;
import com.tank.mapper.IntegerStateMap;
import com.tank.stream.IntegerStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * flink stream demo
 *
 * @author fuchun
 */
public class Stream {
  public static void main(String[] args) {
    final StreamHandler streamHandler = new StreamHandler();
    streamHandler.doAction(env -> {
      env.addSource(new IntegerStream())
          .map(new MapFunction<Tuple2<Integer, Long>, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(Tuple2<Integer, Long> value) throws Exception {
              return new Tuple3<>("1", value.f0, value.f1);
            }
          })
          .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Integer, Long> element) {
              return element.f2;
            }
          })
          .keyBy(0)
          .map(new IntegerStateMap())
          .keyBy(0)
          .sum(1)
          .print();

    });
  }
}
