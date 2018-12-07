package com.tank.stream;

import lombok.val;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author fuchun
 */
public class IntegerStream implements SourceFunction<Tuple2<Integer, Long>> {


  @Override
  public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
    final Random random = new Random();
    while (isContinue) {
      val data = random.nextInt(10);
      ctx.collect(new Tuple2<>(data, System.currentTimeMillis()));
      Thread.sleep(200);
    }
  }

  @Override
  public void cancel() {
    this.isContinue = false;
  }

  private volatile boolean isContinue = true;


}
