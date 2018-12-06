package com.tank;

import com.tank.domain.Item;
import com.tank.domain.Order;
import com.tank.handler.StreamHandler;
import com.tank.stream.ItemStream;
import com.tank.stream.OrderStream;
import lombok.val;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Objects;

/**
 * flink stream demo
 *
 * @author fuchun
 */
public class Stream {
  public static void main(String[] args) {
    StreamHandler streamHandler = new StreamHandler();
    streamHandler.doAction(env -> {

      final DataStream<Tuple2<String, Order>> orderStream = env.addSource(new OrderStream()).map(new MapFunction<String, Order>() {
        final ObjectMapper jsonRw = new ObjectMapper();

        @Override
        public Order map(String jsonStr) throws Exception {
          val order = jsonRw.readValue(jsonStr, Order.class);
          return order;
        }
      }).filter(order -> !Objects.isNull(order.getOrderNo())).map(new MapFunction<Order, Tuple2<String, Order>>() {
        @Override
        public Tuple2<String, Order> map(Order order) throws Exception {
          return new Tuple2<>(order.getOrderNo(), order);
        }
      }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Order>>() {
        @Override
        public long extractAscendingTimestamp(Tuple2<String, Order> element) {
          return element.f1.getTimeStamp();
        }
      }).keyBy(0);

      final DataStream<Tuple2<String, Item>> itemStream = env.addSource(new ItemStream()).map(new MapFunction<String, Item>() {
        final ObjectMapper jsonMapper = new ObjectMapper();

        @Override
        public Item map(String jsonStr) throws Exception {
          return jsonMapper.readValue(jsonStr, Item.class);
        }
      }).filter(item -> !Objects.isNull(item.getOrderNo())).map(new MapFunction<Item, Tuple2<String, Item>>() {
        @Override
        public Tuple2<String, Item> map(Item item) throws Exception {
          return new Tuple2<>(item.getOrderNo(), item);
        }
      }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Item>>() {
        @Override
        public long extractAscendingTimestamp(Tuple2<String, Item> element) {
          return element.f1.getTimeStamp();
        }
      }).keyBy(0);


      DataStream<Tuple2<String, String>> joinedStream = orderStream.join(itemStream).where(new KeySelector<Tuple2<String, Order>, String>() {
        @Override
        public String getKey(Tuple2<String, Order> order) throws Exception {
          return order.f0;
        }
      }).equalTo(new KeySelector<Tuple2<String, Item>, String>() {
        @Override
        public String getKey(Tuple2<String, Item> item) throws Exception {
          return item.f0;
        }
      }).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new JoinFunction<Tuple2<String, Order>, Tuple2<String, Item>, Tuple2<String, String>>() {
        @Override
        public Tuple2<String, String> join(Tuple2<String, Order> first, Tuple2<String, Item> second) throws Exception {
          return new Tuple2<>("order", first.f0);
        }
      });

      joinedStream.print();

    });
  }
}
