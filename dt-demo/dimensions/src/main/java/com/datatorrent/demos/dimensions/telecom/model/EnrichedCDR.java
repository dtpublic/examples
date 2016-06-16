/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo;
import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo.LocationInfo;
import com.datatorrent.demos.dimensions.telecom.generate.MNCRepo;
import com.datatorrent.demos.dimensions.telecom.generate.TACRepo;

/**
 * Append other information - DR - carrier - manufacture, model
 * 
 * @author bright
 *
 */
public class EnrichedCDR extends CallDetailRecord implements BytesSupport
{
  private String drLabel;
  private String operatorCode;
  private String deviceBrand;
  private String deviceModel;
  private String zipCode;

  private String stateCode;
  private String state;
  private String city;

  public EnrichedCDR()
  {
  }

  public static EnrichedCDR fromCallDetailRecord(String line)
  {
    EnrichedCDR enrichedCDR = new EnrichedCDR();
    enrichedCDR.setFromLine(line);
    enrichedCDR.enrich();
    return enrichedCDR;
  }

  public static EnrichedCDR fromCallDetailRecord(CallDetailRecord cdr)
  {
    EnrichedCDR enrichedCDR = new EnrichedCDR();
    enrichedCDR.setFromCdr(cdr);
    enrichedCDR.enrich();
    return enrichedCDR;
  }

  public EnrichedCDR(Map<String, byte[]> nameValueMap)
  {
    super(nameValueMap);

    setDrLabel(Bytes.toString(nameValueMap.get("drLabel")));
    setOperatorCode(Bytes.toString(nameValueMap.get("operatorCode")));
    setDeviceBrand(Bytes.toString(nameValueMap.get("deviceBrand")));
    setDeviceModel(Bytes.toString(nameValueMap.get("deviceModel")));

  }

  protected void enrich()
  {
    //DR
    if (getDr() != 0) {
      drLabel = DisconnectReason.fromCode(getDr()).getLabel();
    }

    //operator code;
    MNCInfo mncInfo = MNCRepo.instance().getMncInfoByImsi(this.getImsi());
    operatorCode = mncInfo.carrier.operatorCode;

    //brand & model
    TACInfo tacInfo = TACRepo.instance().getTacInfoByImei(this.getImei());
    deviceBrand = tacInfo.manufacturer;
    deviceModel = tacInfo.model;

    //enrich location info
    LocationInfo li = LocationRepo.instance().getCloseLocationInfo(getLat(), getLon());
    //zip code
    zipCode = li.getZipCodeAsString();
    stateCode = li.stateCode;
    state = li.state;
    city = li.city;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(delimiter);

    //DR
    if (drLabel != null) {
      sb.append(drLabel);
    }
    sb.append(delimiter);

    sb.append(operatorCode).append(delimiter);

    sb.append(deviceBrand).append(delimiter);
    sb.append(deviceModel).append(delimiter);

    sb.append(zipCode).append(delimiter);
    sb.append(stateCode).append(delimiter);
    sb.append(state).append(delimiter);
    sb.append(city);

    return sb.toString();
  }

  @Override
  public byte[] toBytes()
  {
    return toLine().getBytes();
  }

  public String toLine()
  {
    return toString() + "\n";
  }

  public String getDrLabel()
  {
    return drLabel;
  }

  public void setDrLabel(String drLabel)
  {
    this.drLabel = drLabel;
  }

  public String getOperatorCode()
  {
    return operatorCode;
  }

  public void setOperatorCode(String operatorCode)
  {
    this.operatorCode = operatorCode;
  }

  public String getDeviceBrand()
  {
    return deviceBrand;
  }

  public void setDeviceBrand(String deviceBrand)
  {
    this.deviceBrand = deviceBrand;
  }

  public String getDeviceModel()
  {
    return deviceModel;
  }

  public void setDeviceModel(String deviceModel)
  {
    this.deviceModel = deviceModel;
  }

  public String getZipCode()
  {
    return zipCode;
  }

  public void setZipCode(String zipCode)
  {
    this.zipCode = zipCode;
  }

  public String getStateCode()
  {
    return stateCode;
  }

  public void setStateCode(String stateCode)
  {
    this.stateCode = stateCode;
  }

  public String getState()
  {
    return state;
  }

  public void setState(String state)
  {
    this.state = state;
  }

  public String getCity()
  {
    return city;
  }

  public void setCity(String city)
  {
    this.city = city;
  }

  /**
   * use 2 letter of zip as the region
   * 
   * @return
   */
  public String getRegionZip2()
  {
    return getZipSubString(2);
  }

  /**
   * use 2 letter of zip as the region
   * 
   * @return
   */
  public String getRegionZip3()
  {
    return getZipSubString(3);
  }

  protected String getZipSubString(int length)
  {
    if (length > zipCode.length()) {
      throw new IllegalArgumentException(
          "The length of the zipCode ( " + zipCode.length() + ") is less than begin length: (" + length + ").");
    }
    return zipCode.substring(0, length);
  }
}
