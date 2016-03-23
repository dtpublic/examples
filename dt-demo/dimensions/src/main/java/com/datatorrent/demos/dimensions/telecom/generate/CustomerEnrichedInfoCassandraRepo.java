/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoCassandraRepo implements CustomerEnrichedInfoProvider
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoCassandraRepo.class);

  private SingleRecord[] customerInfoArray;

  private DataWarehouseConfig dataWarehouseConfig = CustomerEnrichedInfoCassandraConfig.instance();

  protected Random random = new Random();

  private static CustomerEnrichedInfoCassandraRepo instance;

  public static CustomerEnrichedInfoCassandraRepo getInstance()
  {
    return instance;
  }

  public static CustomerEnrichedInfoCassandraRepo createInstance(DataWarehouseConfig conf)
  {
    if (instance == null) {
      synchronized (CustomerEnrichedInfoCassandraRepo.class) {
        if (instance == null) {
          instance = new CustomerEnrichedInfoCassandraRepo();
          instance.setDataWarehouseConfig(conf);

          try {
            instance.load();
          } catch (Exception e) {
            logger.error("Can't load CustomerEnrichedInfo. check config: {}. exception: {}.", conf, e.getMessage());
            instance = null;
          }
        }
      }
    } else {
      if (!instance.dataWarehouseConfig.equals(conf)) {
        throw new IllegalArgumentException("CustomerEnrichedInfoRepo suppose only load from same datasource.");
      }
    }
    return instance;
  }

  private CustomerEnrichedInfoCassandraRepo()
  {
  }

  protected void createSession()
  {

  }

  protected void load() throws ClassNotFoundException, SQLException
  {
    Cluster cluster = Cluster.builder().addContactPoint(dataWarehouseConfig.getHost()).build();
    Session session = cluster.connect(dataWarehouseConfig.getDatabase());

    List<SingleRecord> customerInfoList = Lists.newArrayList();

    try {
      ResultSet rs = session
          .execute("select id, imsi, isdn, imei, operatorName, operatorCode, deviceBrand, deviceModel from "
              + dataWarehouseConfig.getDatabase() + "." + dataWarehouseConfig.getTableName());

      Map<String, String> nameValueMap = new HashMap<String, String>();

      Iterator<Row> rowIter = rs.iterator();
      while (!rs.isFullyFetched() && rowIter.hasNext()) {
        Row row = rowIter.next();
        nameValueMap.put("id", row.getString(0));
        nameValueMap.put("imsi", row.getString(1));
        nameValueMap.put("isdn", row.getString(2));
        nameValueMap.put("imei", row.getString(3));
        nameValueMap.put("operatorName", row.getString(4));
        nameValueMap.put("operatorCode", row.getString(5));
        nameValueMap.put("deviceBrand", row.getString(6));
        nameValueMap.put("deviceModel", row.getString(7));

        SingleRecord record = new SingleRecord(nameValueMap);
        customerInfoList.add(record);
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (session != null)
        session.close();
    }

    customerInfoArray = customerInfoList.toArray(new SingleRecord[0]);
  }

  @Override
  public SingleRecord getRandomCustomerEnrichedInfo()
  {
    int index = random.nextInt(customerInfoArray.length);
    return customerInfoArray[index];
  }

  public DataWarehouseConfig getDataWarehouseConfig()
  {
    return dataWarehouseConfig;
  }

  public void setDataWarehouseConfig(DataWarehouseConfig dataWarehouseConfig)
  {
    this.dataWarehouseConfig = dataWarehouseConfig;
  }

}
