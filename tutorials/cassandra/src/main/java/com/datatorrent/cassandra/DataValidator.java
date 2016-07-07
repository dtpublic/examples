/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Does data integrity check.<br/>
 * Upstream operators forwards the cumulative checksum of input data. The
 * checksum is validated against calculated checksum on data received.
 *
 */
public class DataValidator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DataValidator.class);
  private static final String statusFile = "Error.log";
  protected transient FileSystem fs;
  private transient FSDataOutputStream out;
  private int tuplesCount;
  private int hashCode;
  private static int processedTuples;
  private boolean validate = false;

  public transient DefaultInputPort<ScanInformation> validationInfoInput = new DefaultInputPort<ScanInformation>()
  {
    @Override
    public void process(ScanInformation scanInfo)
    {
      if (isValidate()) {
        if (processedTuples >= tuplesCount) {
          if (hashCode == scanInfo.getHashCode()) {
            LOG.info("Data validation is successsful.");
          } else {
            LOG.info("Data validation failed.");
            try {
              out.writeBytes("Error: Generated data doesn't match with data read by JDBC input operator.");
              out.flush();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }
  };

  public transient DefaultInputPort<Object> pojoInput = new DefaultInputPort<Object>()
  {

    @Override
    public void process(Object data)
    {
      if (isValidate()) {
        processedTuples++;
        hashCode = hashCode ^ data.hashCode();
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      Path statusPath = new Path(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + statusFile);
      Configuration configuration = new Configuration();
      fs = FileSystem.newInstance(statusPath.toUri(), configuration);
      out = fs.create(statusPath, true);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      if (out != null) {
        out.close();
      }
      if (fs != null) {
        fs.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to close stream " + e);
    }
  }

  /**
   * Get tuples count
   * 
   * @return tuplesCount
   */
  public int getTuplesCount()
  {
    return tuplesCount;
  }

  /**
   * Set tuples count
   * 
   * @param tuplesCount
   */
  public void setTuplesCount(int tuplesCount)
  {
    this.tuplesCount = tuplesCount;
  }

  /**
   * Get should validate?
   * 
   * @return validate
   */
  public boolean isValidate()
  {
    return validate;
  }

  /**
   * Set should validate?
   * 
   * @param validate
   */
  public void setValidate(boolean validate)
  {
    this.validate = validate;
  }
}
