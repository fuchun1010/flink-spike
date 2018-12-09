package com.tank.handler;

import lombok.val;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * @author fuchun
 */
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(5000);

    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);

    env.getCheckpointConfig().setCheckpointTimeout(60000);

    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    val checkPointUri = "hdfs://10.8.13.97:8020/flink/checkpoints";
    env.setStateBackend(new FsStateBackend(checkPointUri));


  }
}
