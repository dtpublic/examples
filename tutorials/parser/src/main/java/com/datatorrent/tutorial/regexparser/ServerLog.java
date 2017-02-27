package com.datatorrent.tutorial.regexparser;

import java.util.Date;

public class ServerLog
{
  private Date date;
  private int id;
  private String signInId;
  private String ipAddress;
  private String serviceId;
  private String accountId;
  private String platform;


  public int getId()
  {
    return id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public Date getDate()
  {
    return date;
  }

  public void setDate(Date date)
  {
    this.date = date;
  }

  public String getSignInId()
  {
    return signInId;
  }

  public void setSignInId(String signInId)
  {
    this.signInId = signInId;
  }

  public String getIpAddress()
  {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress)
  {
    this.ipAddress = ipAddress;
  }

  public String getServiceId()
  {
    return serviceId;
  }

  public void setServiceId(String serviceId)
  {
    this.serviceId = serviceId;
  }

  public String getAccountId()
  {
    return accountId;
  }

  public void setAccountId(String accountId)
  {
    this.accountId = accountId;
  }

  public String getPlatform()
  {
    return platform;
  }

  public void setPlatform(String platform)
  {
    this.platform = platform;
  }

  @Override
  public String toString()
  {
    return "ServerLog{" +
      "date=" + date +
      ", id=" + id +
      ", signInId='" + signInId + '\'' +
      ", ipAddress='" + ipAddress + '\'' +
      ", serviceId='" + serviceId + '\'' +
      ", accountId='" + accountId + '\'' +
      ", platform='" + platform + '\'' +
      '}';
  }
}