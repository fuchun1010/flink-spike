package com.tank.stream;

import com.tank.domain.Item;
import com.tank.domain.Sleep;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author fuchun
 */
public class ItemStream implements SourceFunction<String> {
  @Override
  public void run(SourceContext<String> ctx) throws Exception {

    while (isContinue) {
      val code = "s00" + sno.getAndIncrement();
      val orders = random.nextInt(5);
      for (int i = 0; i < orders; i++) {
        val item = new Item();
        item.setOrderNo(code);
        item.setName("banana");
        item.setItemCode("i00" + itemNo.getAndIncrement());
        item.setTimeStamp(System.currentTimeMillis());
        ctx.collect(jsonRw.writeValueAsString(item));
      }

      Thread.sleep(Sleep.sleepTime);
    }

  }

  @Override
  public void cancel() {
    this.isContinue = false;
  }

  private volatile boolean isContinue = true;

  private Random random = new Random();
  
  private final AtomicInteger itemNo = new AtomicInteger();

  private final ObjectMapper jsonRw = new ObjectMapper();

  private final AtomicInteger sno = new AtomicInteger();
}
