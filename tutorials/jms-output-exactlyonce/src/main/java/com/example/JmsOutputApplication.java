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
package com.example;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.jms.AbstractJMSSinglePortOutputOperator;

@ApplicationAnnotation(name = "JmsOutputApplication")
public class JmsOutputApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    BatchSequenceGenerator batchSequenceGenerator = dag.addOperator("sequenceGenerator", BatchSequenceGenerator.class);
    PassthroughFailOperator passthroughFailOperator = dag.addOperator("passthrough", PassthroughFailOperator.class);
    StringMessageJMSSinglePortOutputOperator jmsOutputFailingOp = dag.addOperator("jmsOutputFailingOp", StringMessageJMSSinglePortOutputOperator.class);
    ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);

    dag.addStream("generatedData", batchSequenceGenerator.out, passthroughFailOperator.input);
    dag.addStream("duplicatedDataStream", passthroughFailOperator.output, console.input, jmsOutputFailingOp.inputPort);
  }
}
