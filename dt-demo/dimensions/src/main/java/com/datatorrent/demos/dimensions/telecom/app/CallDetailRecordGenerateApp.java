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
import com.datatorrent.demos.dimensions.telecom.operator.CDRHdfsOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CallDetailRecordGenerateOperator;

/**
 * 
 * This application generate CallDetailRecord and save to files - This
 * application will suspend generate tuples when the file reach to 240 files.
 * 
 * @author bright
 *
 */
@ApplicationAnnotation(name = "CallDetailRecordGenerateApp")
public class CallDetailRecordGenerateApp implements StreamingApplication
{
  protected String cdrDir;
  protected long maxFileLength = 0;
  protected String fileName;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    CallDetailRecordGenerateOperator generator = new CallDetailRecordGenerateOperator();
    dag.addOperator("CDR-Generator", generator);

    CDRHdfsOutputOperator writer = new CDRHdfsOutputOperator();
    writer.setFilePath(cdrDir == null ? TelecomDemoConf.instance.getCdrDir() : cdrDir);
    if (maxFileLength > 0) {
      writer.setMaxLength(maxFileLength);
    }
    if (fileName != null && !fileName.isEmpty()) {
      writer.setOutputFileName(fileName);
    }
    dag.addOperator("CDR-Writer", writer);

    dag.addStream("CDR-Stream", generator.bytesOutputPort, writer.input);
  }

  public String getCdrDir()
  {
    return cdrDir;
  }

  public void setCdrDir(String cdrDir)
  {
    this.cdrDir = cdrDir;
  }

}
