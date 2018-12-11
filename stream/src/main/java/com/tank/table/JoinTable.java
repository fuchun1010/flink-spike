package com.tank.table;

import com.tank.domain.Item;
import com.tank.domain.Order;
import com.tank.handler.JsonConverter;
import com.tank.handler.StreamHandler;
import com.tank.stream.ItemStream;
import com.tank.stream.OrderStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Objects;

/**
 * some issue for inner join result , I'd better inspect orderstream and itemstream
 *
 * @author fuchun
 */
public class JoinTable {

  public void joinTable() {
    final StreamHandler streamHandler = new StreamHandler();

    streamHandler.doAction((StreamExecutionEnvironment env) -> {

      final DataStream<Order> orderDataStream = env.addSource(new OrderStream())
          .map(jsonStr -> JsonConverter.toObj(jsonStr, Order.class))
          .filter(order -> !Objects.isNull(order)).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
            @Override
            public long extractAscendingTimestamp(Order element) {
              return element.getTimeStamp();
            }
          }).keyBy("orderNo");

      final DataStream<Item> itemStream = env.addSource(new ItemStream())
          .map(jsonStr -> JsonConverter.toObj(jsonStr, Item.class))
          .filter(item -> !Objects.isNull(item)).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Item>() {
            @Override
            public long extractAscendingTimestamp(Item element) {
              return element.getTimeStamp();
            }
          }).keyBy("orderNo");

      final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


      tableEnv.registerDataStreamInternal("orders", orderDataStream);
      tableEnv.registerDataStream("items", itemStream);


      final Table rs = tableEnv.sqlQuery("select t1.orderNo as orderNo, count(*) as cnt from orders t1 inner join items t2 on t1.orderNo = t2.orderNo group by t1.orderNo");

      final TupleTypeInfo<Tuple2<String, Long>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.LONG());

      DataStream<Tuple2<Boolean, Tuple2<String, Long>>> joinStream = tableEnv.toRetractStream(rs, TypeInformation.of(new ResultRowType()));
      DataStream<Tuple2<String, Long>> cleanResult = joinStream.filter(new FilterFunction<Tuple2<Boolean, Tuple2<String, Long>>>() {
        @Override
        public boolean filter(Tuple2<Boolean, Tuple2<String, Long>> value) throws Exception {
          return value.f0;
        }
      }).map(new MapFunction<Tuple2<Boolean, Tuple2<String, Long>>, Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> map(Tuple2<Boolean, Tuple2<String, Long>> value) throws Exception {
          return value.f1;
        }
      });
      cleanResult.print();

    });

  }

  private static class ResultRowType extends TypeHint<Tuple2<String, Long>> {
    @Override
    public TypeInformation<Tuple2<String, Long>> getTypeInfo() {
      return super.getTypeInfo();
    }
  }
}







