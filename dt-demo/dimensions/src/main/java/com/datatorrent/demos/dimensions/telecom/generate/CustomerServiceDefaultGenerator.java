/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService.IssueType;

public class CustomerServiceDefaultGenerator implements Generator<CustomerService>
{
  public static enum RepoType
  {
    Embeded, HBase, Cassandra
  }

  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceDefaultGenerator.class);
  public static final int MAX_DURATION = 100;
  protected CustomerEnrichedInfoProvider customerEnrichedInfoProvider = null;
  protected RepoType repoType = RepoType.Embeded;

  @Override
  public CustomerService next()
  {
    if (customerEnrichedInfoProvider == null) {
      customerEnrichedInfoProvider = createCustomerEnrichedInfoProvider();
    }

    SingleRecord ci = customerEnrichedInfoProvider.getRandomCustomerEnrichedInfo();
    String imsi = ci.imsi;
    String imei = ci.imei;
    String isdn = ci.isdn;

    int totalDuration = Generator.random.nextInt(MAX_DURATION);
    int wait = (int)(totalDuration * Math.random());
    String zipCode = LocationRepo.instance().getRandomZipCode();
    IssueType issueType = IssueType.values()[Generator.random.nextInt(IssueType.values().length)];
    boolean satisfied = (Generator.random.nextInt(2) == 0);
    return new CustomerService(imsi, isdn, imei, totalDuration, wait, zipCode, issueType, satisfied);
  }

  protected CustomerEnrichedInfoProvider createCustomerEnrichedInfoProvider()
  {
    if (RepoType.HBase == repoType) {
      customerEnrichedInfoProvider = CustomerEnrichedInfoHbaseRepo.createInstance(CustomerEnrichedInfoHBaseConfig.instance());
    } else if (RepoType.Cassandra == repoType) {
      customerEnrichedInfoProvider = CustomerEnrichedInfoCassandraRepo.createInstance(CustomerEnrichedInfoCassandraConfig.instance());
    } else {
      // default (RepoType.Embeded == repoType)
      customerEnrichedInfoProvider = CustomerEnrichedInfoEmbededRepo.instance();
    }
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
    if (this.repoType == null) {
      this.repoType = RepoType.Embeded;
    }
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
