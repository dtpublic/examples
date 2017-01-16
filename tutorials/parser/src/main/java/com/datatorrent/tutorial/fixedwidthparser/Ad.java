package com.datatorrent.tutorial.fixedwidthparser;

import java.util.Date;

public class Ad
{

  private int adId;
  private int campaignId;
  private String adName;
  private double bidPrice;
  private Date startDate;
  private Date endDate;
  private long securityCode;
  private boolean active;
  private boolean optimized;
  private String parentCampaign;
  private Character weatherTargeted;
  private String valid;

  public Ad()
  {

  }

  public int getAdId()
  {
    return adId;
  }

  public void setAdId(int adId)
  {
    this.adId = adId;
  }

  public int getCampaignId()
  {
    return campaignId;
  }

  public void setCampaignId(int campaignId)
  {
    this.campaignId = campaignId;
  }

  public String getAdName()
  {
    return adName;
  }

  public void setAdName(String adName)
  {
    this.adName = adName;
  }

  public double getBidPrice()
  {
    return bidPrice;
  }

  public void setBidPrice(double bidPrice)
  {
    this.bidPrice = bidPrice;
  }

  public long getSecurityCode()
  {
    return securityCode;
  }

  public void setSecurityCode(long securityCode)
  {
    this.securityCode = securityCode;
  }

  public boolean isActive()
  {
    return active;
  }

  public void setActive(boolean active)
  {
    this.active = active;
  }

  public boolean isOptimized()
  {
    return optimized;
  }

  public void setOptimized(boolean optimized)
  {
    this.optimized = optimized;
  }

  public String getParentCampaign()
  {
    return parentCampaign;
  }

  public void setParentCampaign(String parentCampaign)
  {
    this.parentCampaign = parentCampaign;
  }

  public Character getWeatherTargeted()
  {
    return weatherTargeted;
  }

  public void setWeatherTargeted(Character weatherTargeted)
  {
    this.weatherTargeted = weatherTargeted;
  }

  public String getValid()
  {
    return valid;
  }

  public void setValid(String valid)
  {
    this.valid = valid;
  }
  public Date getEndDate()
  {
    return endDate;
  }

  public void setEndDate(Date endDate)
  {
    this.endDate = endDate;
  }
  public Date getStartDate()
  {
    return startDate;
  }

  public void setStartDate(Date startDate)
  {
    this.startDate = startDate;
  }


  @Override
  public String toString()
  {
    return "Ad [adId=" + adId + ", campaignId=" + campaignId + ", adName=" + adName + ", bidPrice=" + bidPrice
      + ", startDate=" + startDate + ", endDate=" + endDate + ", securityCode=" + securityCode + ", active="
      + active + ", optimized=" + optimized + ", parentCampaign=" + parentCampaign + ", weatherTargeted="
      + weatherTargeted + ", valid=" + valid + "]";
  }
}
