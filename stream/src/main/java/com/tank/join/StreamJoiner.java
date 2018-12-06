package com.tank.join;

import com.tank.domain.Item;
import com.tank.domain.Order;
import com.tank.stream.ItemStream;
import com.tank.stream.OrderStream;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.Objects;

/**
 * @author fuchun
 */
public class StreamJoiner {

  /**
   * @param env
   * @return
   */
  public DataStream<Tuple2<String, Order>> generateOrderStream(final StreamExecutionEnvironment env) {
    Objects.requireNonNull(env);
    final DataStream<Tuple2<String, Order>> orderStream = env.addSource(new OrderStream()).map(new MapFunction<String, Order>() {
      final ObjectMapper jsonRw = new ObjectMapper();

      @Override
      public Order map(String jsonStr) throws Exception {
        val order = jsonRw.readValue(jsonStr, Order.class);
        return order;
      }
    }).uid("orderMap").filter(order -> !Objects.isNull(order.getOrderNo())).map(new MapFunction<Order, Tuple2<String, Order>>() {
      @Override
      public Tuple2<String, Order> map(Order order) throws Exception {
        return new Tuple2<>(order.getOrderNo(), order);
      }
    }).uid("orderFilter").assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Order>>() {
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

    return orderStream;
  }


  public DataStream<Tuple2<String, Item>> generateItemStream(final StreamExecutionEnvironment env) {
    final DataStream<Tuple2<String, Item>> itemStream = env.addSource(new ItemStream())
        .map(new MapFunction<String, Item>() {
          final ObjectMapper jsonMapper = new ObjectMapper();

          @Override
          public Item map(String jsonStr) throws Exception {
            return jsonMapper.readValue(jsonStr, Item.class);
          }
        })
        .uid("itemMapToJson")
        .filter(item -> !Objects.isNull(item.getOrderNo()))
        .uid("itemFilter")
        .map(new MapFunction<Item, Tuple2<String, Item>>() {
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
    return itemStream;
  }


}
