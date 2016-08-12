package com.datatorrent.tutorial.csvparser;

import java.util.Date;

public class Campaign
{
  private int adId;
  private String adName;
  private String campaignName;
  private String parentCampaign;
  private double campaignBudget;
  private double bidPrice;
  private int campaignId;
  private long securityCode;
  private boolean weatherTargeting;
  private boolean active;
  private boolean optimized;
  private Date startDate;
  private Date endDate;

  public int getAdId()
  {
    return adId;
  }

  public void setAdId(int adId)
  {
    this.adId = adId;
  }

  public String getCampaignName()
  {
    return campaignName;
  }

  public void setCampaignName(String campaignName)
  {
    this.campaignName = campaignName;
  }

  public String getParentCampaign()
  {
    return parentCampaign;
  }

  public void setParentCampaign(String parentCampaign)
  {
    this.parentCampaign = parentCampaign;
  }

  public double getCampaignBudget()
  {
    return campaignBudget;
  }

  public void setCampaignBudget(double campaignBudget)
  {
    this.campaignBudget = campaignBudget;
  }

  public boolean isWeatherTargeting()
  {
    return weatherTargeting;
  }

  public void setWeatherTargeting(boolean weatherTargeting)
  {
    this.weatherTargeting = weatherTargeting;
  }

  public Date getStartDate()
  {
    return startDate;
  }

  public void setStartDate(Date startDate)
  {
    this.startDate = startDate;
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

  public Date getEndDate()
  {
    return endDate;
  }

  public void setEndDate(Date endDate)
  {
    this.endDate = endDate;
  }

  @Override
  public String toString()
  {
    return "Campaign [adId=" + adId + ", adName=" + adName + ", campaignName=" + campaignName + ", parentCampaign="
        + parentCampaign + ", campaignBudget=" + campaignBudget + ", bidPrice=" + bidPrice + ", campaignId="
        + campaignId + ", securityCode=" + securityCode + ", weatherTargeting=" + weatherTargeting + ", active="
        + active + ", optimized=" + optimized + ", startDate=" + startDate + ", endDate=" + endDate + "]";
  }
}
