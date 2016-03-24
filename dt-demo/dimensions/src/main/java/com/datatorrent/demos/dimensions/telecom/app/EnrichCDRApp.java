/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.operator.CDREnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CDRHdfsInputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CallDetailRecordGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRHbaseOutputOperator;

/**
 * 
 * This application read Call Detail Record from files, enrich the CDR records
 * and save to the output
 *   - This application will remove the oldest files but keep the files not less than 120.
 * 
 * @author bright
 *
 */

@ApplicationAnnotation(name = "EnrichCDRApp")
public class EnrichCDRApp implements StreamingApplication {
  protected String cdrDir;

  private String filePatternRegexp = ".*cdr\\.\\d+\\z";
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    CDRHdfsInputOperator reader = new CDRHdfsInputOperator();
    reader.setDirectory(cdrDir==null ? TelecomDemoConf.instance.getCdrDir() : cdrDir);
    reader.getScanner().setFilePatternRegexp(filePatternRegexp);
    dag.addOperator("CDR-Reader", reader);

    CDREnrichOperator enrichOperator = new CDREnrichOperator();
    dag.addOperator("CDR-Enrich", enrichOperator);
    
    EnrichedCDRHbaseOutputOperator outputOperator = new EnrichedCDRHbaseOutputOperator();
    dag.addOperator("EnrichedCDR-output", outputOperator);
    
    //streams
    dag.addStream("CDR-Stream", reader.output, enrichOperator.stringInputPort);
    dag.addStream("EnrichedCDR-Stream", enrichOperator.outputPort, outputOperator.input);
  }

  public String getCdrDir() {
    return cdrDir;
  }

  public void setCdrDir(String cdrDir) {
    this.cdrDir = cdrDir;
  }

  public String getFilePatternRegexp() {
    return filePatternRegexp;
  }

  public void setFilePatternRegexp(String filePatternRegexp) {
    this.filePatternRegexp = filePatternRegexp;
  }

  
}
