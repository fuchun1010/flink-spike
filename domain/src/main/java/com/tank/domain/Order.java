package com.tank.domain;

import lombok.Data;

/**
 * @author fuchun
 */
@Data
public class Order {

  private String orderNo;

  private String storeCode;

  private double price;

  private long timeStamp;

  @Override
  public String toString() {
    return String.format("order -> order No is: %s", orderNo);
  }

}
