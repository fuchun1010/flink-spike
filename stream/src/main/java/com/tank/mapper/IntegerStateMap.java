package com.tank.mapper;

import lombok.val;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Objects;

/**
 * @author fuchun
 */
public class IntegerStateMap extends RichMapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {


  @Override
  public void open(Configuration parameters) throws Exception {
    val stateDescriptor = new ValueStateDescriptor<Tuple3<String, Integer, Long>>("rs", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {
    }));
    sum = this.getRuntimeContext().getState(stateDescriptor);
  }


  private transient ValueState<Tuple3<String, Integer, Long>> sum;

  @Override
  public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
    val data = Objects.isNull(sum.value()) ? new Tuple3<>("rs", 0, 0L) : sum.value();
    val timeStamp = value.f2;

    data.f1 += value.f1;
    data.f2 = timeStamp;

    sum.update(data);

    return new Tuple3<>("rs", value.f1, value.f2);
  }
}
