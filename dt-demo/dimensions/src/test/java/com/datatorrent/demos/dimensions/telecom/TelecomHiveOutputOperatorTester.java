/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.hive.HiveOperator;
import com.datatorrent.contrib.hive.HiveStore;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHiveConfig;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.hive.TelecomHiveExecuteOperator;
import com.datatorrent.demos.dimensions.telecom.hive.TelecomHiveOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CallDetailRecordGenerateOperator;

public class TelecomHiveOutputOperatorTester
{
  public static final String FILE_DIR = "target/Hive.bak";
  public static final String FILE_NAME = "hivedata";
  public static final String tablemap = "tempmap";
  public static String delimiterMap = ":";
  public static final String HOST = "localhost";
  public static final String PORT = "10000";
  //public static final String DATABASE = "default";
  public static final String HOST_PREFIX = "jdbc:hive://";
  private static final String user = "bright";
  private static final String pass = "";

  protected long runTime = 60000;

  @Before
  public void setUp()
  {
    TelecomDemoConf.instance.setCassandraHost("localhost");
    TelecomDemoConf.instance.setHiveHost("localhost");
    TelecomDemoConf.instance.setHiveUserName("bright");
    TelecomDemoConf.instance.setDatabase("default");

    CustomerEnrichedInfoCassandraConfig.instance().setHost("localhost");
    CustomerEnrichedInfoCassandraConfig.instance().setDatabase("TelecomDemo");

  }

  @Test
  public void test() throws Exception
  {
    //CustomerEnrichedInfoHBaseConfig.instance().setHost("localhost");
    TelecomDemoConf.instance.setCdrDir(FILE_DIR);

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);

    populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(runTime);

    lc.shutdown();
  }

  public void populateDAG(DAG dag, Configuration conf)
  {
    CallDetailRecordGenerateOperator generator = new CallDetailRecordGenerateOperator();
    dag.addOperator("CDR-Generator", generator);

    TelecomHiveOutputOperator hiveOutput = new TelecomHiveOutputOperator();
    hiveOutput.setFilePath(FILE_DIR);
    hiveOutput.setOutputFileName(FILE_NAME);
    hiveOutput.setMaxLength(1024 * 1024);
    hiveOutput.setFilePermission((short)511);

    dag.addOperator("hiveOutput", hiveOutput);
    dag.addStream("CDR-Stream", generator.cdrOutputPort, hiveOutput.input);

    TelecomHiveExecuteOperator hiveOperator = new TelecomHiveExecuteOperator();

    {
      HiveStore hiveStore = createStore(null);
      hiveStore.setFilepath(FILE_DIR);
      hiveOperator.setHivestore(hiveStore);
    }
    hiveOperator.setHiveConfig(EnrichedCDRHiveConfig.instance());
    hiveOperator.setTablename(tablemap);

    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    dag.addOperator("hiveOperator", hiveOperator);
    dag.addStream("hiveCmd", hiveOutput.hiveCmdOutput, hiveOperator.input);
  }

  public static HiveStore createStore(HiveStore hiveStore)
  {
    String host = HOST;
    String port = PORT;

    if (hiveStore == null) {
      hiveStore = new HiveStore();
    }

    StringBuilder sb = new StringBuilder();
    String tempHost = HOST_PREFIX + host + ":" + port;

    sb.append("user:").append("").append(user);
    sb.append("password:").append(pass);

    String properties = sb.toString();
    hiveStore.setDatabaseDriver("org.apache.hive.jdbc.HiveDriver");

    hiveStore.setDatabaseUrl("jdbc:hive2://localhost:10000");
    hiveStore.setUserName(user);
    hiveStore.setPassword(pass);
    hiveStore.setConnectionProperties(properties);
    return hiveStore;
  }
}
