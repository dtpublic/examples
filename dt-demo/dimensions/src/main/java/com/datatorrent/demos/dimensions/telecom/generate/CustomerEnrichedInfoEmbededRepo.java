/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;

import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoEmbededRepo implements CustomerEnrichedInfoProvider
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoEmbededRepo.class);

  protected static final String FILE_NAME = "customerenrichedinfo.csv";

  protected SingleRecord[] customerInfoArray;

  protected Random random = new Random();

  private static CustomerEnrichedInfoEmbededRepo instance;

  public static CustomerEnrichedInfoEmbededRepo instance()
  {
    if (instance == null) {
      synchronized (CustomerEnrichedInfoEmbededRepo.class) {
        if (instance == null) {
          instance = new CustomerEnrichedInfoEmbededRepo();

          try {
            instance.load();
          } catch (Exception e) {
            logger.error("Can't load CustomerEnrichedInfo. exception: {}.", e.getMessage());
            instance = null;
          }
        }
      }
    }
    return instance;
  }

  private CustomerEnrichedInfoEmbededRepo()
  {
  }

  protected void load() throws ClassNotFoundException, SQLException
  {
    InputStream is = null;
    BufferedReader br = null;
    List<SingleRecord> customerInfoList = Lists.newArrayList();
    try {
      is = this.getClass().getClassLoader().getResourceAsStream(FILE_NAME);

      br = new BufferedReader(new InputStreamReader(is));

      final Map<String, String> nameValueMap = new HashMap<String, String>();
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }

        Map<String, String> result = setValue(line, nameValueMap);
        if (result == null) {
          continue;
        }
        SingleRecord record = new SingleRecord(result);
        customerInfoList.add(record);
      }

    } catch (Exception e) {
      logger.error("load() exception.", e);
    } finally {
      if (br != null) {
        IOUtils.closeQuietly(br);
      }
      if (is != null) {
        IOUtils.closeQuietly(is);
      }
    }

    customerInfoArray = customerInfoList.toArray(new SingleRecord[0]);
  }

  /**
   * id text PRIMARY KEY, devicebrand text, devicemodel text, imei text, imsi
   * text, isdn text, operatorcode text, operatorname text
   *
   * 13957,Nokia,6310i,351488202684829,310070659757953,019250798813,ATT,AT&T
   * 27862,Apple,iPhone,012158009912128,310170773497249,017076623366,TMO,T-
   * Mobile
   * 
   * @param line
   * @param nameValueMap
   * @return
   */
  public Map<String, String> setValue(String line, Map<String, String> nameValueMap)
  {
    line = line.trim();
    if (line.isEmpty() || line.startsWith("#")) {
      return null;
    }
    String[] items = line.split(",");
    try {
      nameValueMap.put("id", items[0]);
      nameValueMap.put("deviceBrand", items[1]);
      nameValueMap.put("deviceModel", items[2]);
      nameValueMap.put("imei", items[3]);
      nameValueMap.put("imsi", items[4]);
      nameValueMap.put("isdn", items[5]);
      nameValueMap.put("operatorCode", items[6]);
      nameValueMap.put("operatorName", items[7]);

      return nameValueMap;
    } catch (Exception e) {
      logger.warn("Invalid line: {}", line);
      return null;
    }
  }

  @Override
  public SingleRecord getRandomCustomerEnrichedInfo()
  {
    int index = random.nextInt(customerInfoArray.length);
    return customerInfoArray[index];
  }
}
