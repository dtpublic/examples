/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.genericdemo.enginedata;

/**
 * A single sales event
 */
public class EngineDataEvent
{
  public long time;
  public String errorCode = "";
  public String model = "";
  public int gear;
  public double temperature;
  public double speed;
  public double rpm;
  public String description = "";

  public EngineDataEvent()
  {
  }

  public long getTime()
  {
    return time;
  }

  public void setTime(long time)
  {
    this.time = time;
  }

  public String getErrorCode()
  {
    return errorCode;
  }

  public void setErrorCode(String errorCode)
  {
    this.errorCode = errorCode;
  }

  public String getModel()
  {
    return model;
  }

  public void setModel(String model)
  {
    this.model = model;
  }

  public int getGear()
  {
    return gear;
  }

  public void setGear(int gear)
  {
    this.gear = gear;
  }

  public double getTemperature()
  {
    return temperature;
  }

  public void setTemperature(double temperature)
  {
    this.temperature = temperature;
  }

  public double getSpeed()
  {
    return speed;
  }

  public void setSpeed(double speed)
  {
    this.speed = speed;
  }

  public double getRpm()
  {
    return rpm;
  }

  public void setRpm(double rpm)
  {
    this.rpm = rpm;
  }

  public String getDescription()
  {
    return description;
  }

  public void setDescription(String description)
  {
    this.description = description;
  }

  @Override
  public String toString()
  {
    return "EngineDataEvent{" + "time=" + time + ", errorCode=" + errorCode + ", model=" + model + ", gear=" + gear + ", temperature=" + temperature + ", speed=" + speed + ", rpm=" + rpm + ", description=" + description + '}';
  }
}
