package com.tank.handler;

import lombok.val;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class StreamHandler {

  public void doAction(StreamAction streamAction) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment();
    this.initEnv(env);
    streamAction.doAction(env);

    try {
      env.execute("job");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void initEnv(final StreamExecutionEnvironment env) {
    Objects.requireNonNull(env);
    //TODO config checkpoint and so on
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    val maxAllowedCores = Runtime.getRuntime().availableProcessors();
    env.setMaxParallelism(maxAllowedCores);

  }
}
