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

}
