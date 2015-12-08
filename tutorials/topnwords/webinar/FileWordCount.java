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
package com.example.myapexapp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.commons.lang.mutable.MutableInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

/**
 * Monitors an input directory for text files, computes word frequency counts per file and globally,
 * and writes the top N pairs to an output file and to snapshot servers for visualization.
 * Currently designed to work with only 1 file at a time; will be enhanced later to support
 * multiple files dropped into the monitored directory at the same time.
 *
 * <p>
 * Receives per-window list of pairs (word, frequency) on the input port. When the end of a file
 * is reached, expects to get an EOF on the control port; at the next endWindow, the sorted list
 * of words and their frequencies is emitted to the output port.
 *
 * Since the EOF is received by a single operator, this operator cannot be partitionable
 *
 * @since 3.2.0
 */
public class FileWordCount extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(FileWordCount.class);

  // set to true when we get an EOF control tuple
  protected boolean eof = false;

  // last component of path (i.e. only file name)
  // incoming value from control tuple
  protected String fileName;

  // wordMapFile   : {word => frequency} map, current file, all words
  protected Map<String, WCPair> wordMapFile = new HashMap<>();

  // singleton map of fileName to sorted list of (word, frequency) pairs
  protected transient Map<String, Object> resultFileFinal;

  // final sorted list of (word,frequency) pairs
  protected transient List<WCPair> fileFinalList;

  public final transient DefaultInputPort<List<WCPair>> input = new DefaultInputPort<List<WCPair>>()
  {
    @Override
    public void process(List<WCPair> list)
    {
      // blend incoming list into wordMapFile and wordMapGlobal
      for (WCPair pair : list) {
        final String word = pair.word;
        WCPair filePair = wordMapFile.get(word);
        if (null != filePair) {    // word seen previously in current file
          filePair.freq += pair.freq;
          continue;
        }

        // new word in current file
        filePair = new WCPair(word, pair.freq);
        wordMapFile.put(word, filePair);
      }
    }
  };

  public final transient DefaultInputPort<String> control = new DefaultInputPort<String>()
  {
    @Override
    public void process(String msg)
    {
      if (msg.isEmpty()) {    // sanity check
        throw new RuntimeException("Empty file path");
      }
      LOG.info("FileWordCount: EOF for {}", msg);
      fileName = msg;
      eof = true;
      // NOTE: current version only supports processing one file at a time.
    }
  };

  // fileOutput -- tuple is singleton map {<fileName> => fileFinalList}; emitted on EOF
  public final transient DefaultOutputPort<Map<String, Object>>
    fileOutput = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    // singleton map {<fileName> => fileFinalList}; cannot populate it yet since we need fileName
    resultFileFinal = new HashMap<>(1);
    fileFinalList = new ArrayList<>();
  }

  @Override
  public void endWindow()
  {
    LOG.info("FileWordCount: endWindow for {}", fileName);

    if (wordMapFile.isEmpty()) {    // no words found
      if (eof) {                    // write empty list to fileOutput port
        // got EOF, so output empty list to output file
        fileFinalList.clear();
        resultFileFinal.put(fileName, fileFinalList);
        fileOutput.emit(resultFileFinal);

        // reset for next file
        eof = false;
        fileName = null;
        resultFileFinal.clear();
      }
      LOG.info("FileWordCount: endWindow for {}, no words", fileName);
      return;
    }

    LOG.info("FileWordCount: endWindow for {}, wordMapFile.size = {}, eof = {}",
             fileName, wordMapFile.size(), eof);

    if (eof) {                     // got EOF earlier
      if (null == fileName) {      // need file name to emit topN pairs to file writer
        throw new RuntimeException("EOF but no fileName at endWindow");
      }

      // sort list from wordMapFile into fileFinalList and emit it
      getList(wordMapFile);
      resultFileFinal.put(fileName, fileFinalList);
      fileOutput.emit(resultFileFinal);

      // reset for next file
      eof = false;
      fileName = null;
      wordMapFile.clear();
      resultFileFinal.clear();
    }
  }

  // populate fileFinalList with topN frequencies from argument
  // This list is suitable input to WordCountWriter which writes it to a file
  // MUST have map.size() > 0 here
  //
  private void getList(final Map<String, WCPair> map)
  {
    fileFinalList.clear();
    fileFinalList.addAll(map.values());

    // sort entries in descending order of frequency
    Collections.sort(fileFinalList, new Comparator<WCPair>() {
        @Override
        public int compare(WCPair o1, WCPair o2) {
          return (int)(o2.freq - o1.freq);
        }
    });
  
    LOG.info("FileWordCount:getList: fileFinalList.size = {}", fileFinalList.size());
  }
}
