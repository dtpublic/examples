/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CallDetailRecord;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

/**
 * This class generate random CDR from customer information
 * @author bright
 *
 */
public class CallDetailRecordCustomerInfoGenerator implements Generator<CallDetailRecord> {
  public static enum RepoType
  {
    Embeded,
    HBase,
    Cassandra
  }
  
  private static final transient Logger logger = LoggerFactory.getLogger(CallDetailRecordCustomerInfoGenerator.class);
  
  protected CustomerEnrichedInfoProvider customerEnrichedInfoProvider = null;
  protected CallDetailRecordRandomGenerator cdrRandomGenerator = new CallDetailRecordRandomGenerator();
  protected RepoType repoType = RepoType.Embeded;
  
  @Override
  public CallDetailRecord next() {
    if(customerEnrichedInfoProvider == null)
      customerEnrichedInfoProvider = createCustomerEnrichedInfoProvider();
      
    
    CallDetailRecord cdr = cdrRandomGenerator.next();
    
    //fill with the customer info.
    SingleRecord customerInfo = customerEnrichedInfoProvider.getRandomCustomerEnrichedInfo();
    cdr.setIsdn(customerInfo.getIsdn());
    cdr.setImsi(customerInfo.getImsi());
    cdr.setImei(customerInfo.getImei());
    
    return cdr;
  }
  
  protected CustomerEnrichedInfoProvider createCustomerEnrichedInfoProvider()
  {
    if(RepoType.HBase == repoType)
      customerEnrichedInfoProvider = CustomerEnrichedInfoHbaseRepo.createInstance(CustomerEnrichedInfoHBaseConfig.instance());
    else if(RepoType.Cassandra == repoType)
      customerEnrichedInfoProvider = CustomerEnrichedInfoCassandraRepo.createInstance(CustomerEnrichedInfoCassandraConfig.instance());
    else    // default (RepoType.Embeded == repoType)
      customerEnrichedInfoProvider = CustomerEnrichedInfoEmbededRepo.instance();
    logger.info("repoType={}, customerEnrichedInfoProvider={}", repoType, customerEnrichedInfoProvider);
    return customerEnrichedInfoProvider;
  }

  public String getRepoType()
  {
    return repoType.name();
  }

  public void setRepoType(String repoType)
  {
    this.repoType = RepoType.valueOf(repoType);
    if(this.repoType == null)
      this.repoType = RepoType.Embeded;
  }

  public CustomerEnrichedInfoProvider getCustomerEnrichedInfoProvider()
  {
    return customerEnrichedInfoProvider;
  }

  public void setCustomerEnrichedInfoProvider(CustomerEnrichedInfoProvider customerEnrichedInfoProvider)
  {
    this.customerEnrichedInfoProvider = customerEnrichedInfoProvider;
  }

  
}
