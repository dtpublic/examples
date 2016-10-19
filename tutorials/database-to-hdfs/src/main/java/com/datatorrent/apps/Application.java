/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.apps;

import java.util.List;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.lib.db.jdbc.JdbcPOJOPollInputOperator;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;

@ApplicationAnnotation(name = "Database-to-HDFS")
public class Application implements StreamingApplication
{
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOPollInputOperator poller = dag.addOperator("JdbcPoller", new JdbcPOJOPollInputOperator());

    JdbcStore store = new JdbcStore();
    poller.setStore(store);
    poller.setFieldInfos(addFieldInfos());

    CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());

    StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", new StringFileOutputOperator());

    dag.addStream("dbrecords", poller.outputPort, formatter.in);
    dag.addStream("string", formatter.out, fileOutput.input);
    dag.setInputPortAttribute(formatter.in, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(fileOutput.input, PortContext.PARTITION_PARALLEL, true);

    /*
     * To add custom logic to your DAG, add your custom operator here with
     * dag.addOperator api call and connect it in the dag using the dag.addStream
     * api call. 
     * 
     * For example: 
     * 
     * To add the transformation operator in the DAG, use the following block of
     * code.
     * 
     * TransformOperator transform = dag.addOperator("Transform", new TransformOperator());
     * Map<String, String> expMap = Maps.newHashMap();
     * expMap.put("name", "{$.name}.toUpperCase()");
     * transform.setExpressionMap(expMap);
     * 
     * And to connect it in the DAG as follows:
     * JdbcPoller --> Transform --> Formatter --> FileOutput
     *
     * Replace the following line:
     * dag.addStream("dbrecords", poller.outputPort, formatter.in);
     * 
     * with the following two lines:
     * dag.addStream("dbrecords", poller.outputPort, transform.input);
     * dag.addStream("transformed", transform.output, formatter.in);
     * 
     */

  }

  /**
   * This method can be modified to have field mappings based on used defined
   * class
   */
  private List<FieldInfo> addFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();

    /*
     * To use this application with custom schema add field info mapping as shown 
     * on the following line:
     * fieldInfos.add(new FieldInfo("DATABASE_COLUMN_NAME", "pojoFieldName", SupportType.DATABASE_COLUMN_TYPE));
     * 
     * Also, update TUPLE_CLASS property from the xml configuration files.
     */
    fieldInfos.add(new FieldInfo("ACCOUNT_NO", "accountNumber", SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("NAME", "name", SupportType.STRING));
    fieldInfos.add(new FieldInfo("AMOUNT", "amount", SupportType.INTEGER));
    return fieldInfos;
  }
}
