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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;

import com.datatorrent.api.DefaultInputPort;

/**
 * ValidationToFile operator:
 *  1. writes all incoming tuples to a validation file on HDFS
 *  2. when no more tuples come in it checks for duplicates and writes the result to the validation file
 */
public class ValidationToFile extends GenericFileOutputOperator.StringFileOutputOperator
{
  int lastMessagesSize;

  //number of windows in which the message size has not changed. Validation will be executed once it becomes less than 0.
  int sameMessageSizeCount = 5;
  List<String> messages = new ArrayList();

  static boolean isValidated = false;

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      messages.add(tuple);
      processTuple(tuple);
    }
  };

  @Override
  public void endWindow()
  {
    if (!isValidated);
    {
      validateJmsInput();
    }
    super.endWindow();
  }

  private void validateJmsInput()
  {
    if (lastMessagesSize == messages.size()) {
      sameMessageSizeCount--;
    } else {
      lastMessagesSize = messages.size();
    }
    if (sameMessageSizeCount < 0) {
      Set<String> messageSet = new HashSet<>(messages);
      if (messages.size() == messageSet.size()) {
        processTuple("Validation successful. No duplicate messages.");
      } else {
        processTuple("Validation failed. Duplicate messages found.");
      }
      isValidated = true;
    }
  }

  public int getSameMessageSizeCount()
  {
    return sameMessageSizeCount;
  }

  public void setSameMessageSizeCount(int sameMessageSizeCount)
  {
    this.sameMessageSizeCount = sameMessageSizeCount;
  }
}
