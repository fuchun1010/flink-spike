package com.tank.stream;

import com.tank.domain.Order;
import com.tank.domain.Sleep;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author fuchun
 */
public class OrderStream implements SourceFunction<String> {

  @Override
  public void run(SourceContext<String> ctx) throws Exception {

    while (isContinue) {
      val order = new Order();
      val code = "s00" + sno.getAndIncrement();
      order.setOrderNo(code);
      val timeStamp = System.currentTimeMillis() - delay.nextInt(2000);
      order.setTimeStamp(timeStamp);
      order.setStoreCode(code);
      order.setPrice(random.nextInt(600));
      val jsonStr = jsonRw.writeValueAsString(order);
      ctx.collect(jsonStr);
      val split = IntStream.range(1, 100).mapToObj(index -> "*").reduce("", (a, b) -> a + b);
      System.out.println(split);
      Thread.sleep(Sleep.sleepTime);
    }

  }

  @Override
  public void cancel() {

    this.isContinue = false;
  }


  private volatile boolean isContinue = true;

  private final AtomicInteger sno = new AtomicInteger();

  private final Random random = new Random();

  private final Random delay = new Random();

  private ObjectMapper jsonRw = new ObjectMapper();

}
