package com.tank;

import com.tank.domain.Item;
import com.tank.domain.Order;
import com.tank.handler.StreamHandler;
import com.tank.join.StreamJoiner;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * flink stream demo
 *
 * @author fuchun
 */
public class Stream {
  public static void main(String[] args) {
    final StreamHandler streamHandler = new StreamHandler();
    final StreamJoiner joiner = new StreamJoiner();
    streamHandler.doAction(env -> {

      final DataStream<Tuple2<String, Order>> orderStream = joiner.generateOrderStream(env);

      final DataStream<Tuple2<String, Item>> itemStream = joiner.generateItemStream(env);

      DataStream<Tuple3<String, Order, Item>> joinedStream = orderStream
          .join(itemStream).where(new KeySelector<Tuple2<String, Order>, String>() {
            @Override
            public String getKey(Tuple2<String, Order> order) throws Exception {
              return order.f0;
            }
          })
          .equalTo(new KeySelector<Tuple2<String, Item>, String>() {
            @Override
            public String getKey(Tuple2<String, Item> item) throws Exception {
              return item.f0;
            }
          })
          .window(TumblingEventTimeWindows.of(Time.seconds(5)))
          .apply(new JoinFunction<Tuple2<String, Order>, Tuple2<String, Item>, Tuple3<String, Order, Item>>() {
            @Override
            public Tuple3<String, Order, Item> join(Tuple2<String, Order> first, Tuple2<String, Item> second) throws Exception {
              return new Tuple3<>(first.f0, first.f1, second.f1);
            }
          });

      joinedStream.print();

      
    });
  }
}
