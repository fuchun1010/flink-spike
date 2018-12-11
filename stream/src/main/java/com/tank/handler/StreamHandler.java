package com.tank.handler;

import lombok.val;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author fuchun
 */
public class StreamHandler {

  public void doAction(StreamAction streamAction) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment();
    log.info("start init StreamExecutionEnvironment");
    this.initEnv(env);
    streamAction.doAction(env);
    log.info("end init StreamExecutionEnvironment");

    try {
      env.execute("job");
    } catch (Exception e) {
      log.error("execute stream exception, message is {}", e.getMessage());
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

//    val checkPointUri = "hdfs://10.8.13.97:8020/flink/checkpoints";
//    env.setStateBackend(new FsStateBackend(checkPointUri));

  }

  private final Logger log = LoggerFactory.getLogger(StreamHandler.class);
}
