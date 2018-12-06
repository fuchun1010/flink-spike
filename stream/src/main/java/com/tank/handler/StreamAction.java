package com.tank.handler;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author fuchun
 */
public interface StreamAction {

  /**
   * @param env
   */
  void doAction(final StreamExecutionEnvironment env);
}
