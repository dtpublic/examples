package com.datatorrent.demos.dimensions.sales.generic;

/**
 * A single sales event
 */
class SalesEvent
{

  /* dimension keys */
  public long time;
  public int productId;
  public String customer;
  public String channel;
  public String region;
  /* metrics */
  public double sales;
  public double discount;
  public double tax;
}
