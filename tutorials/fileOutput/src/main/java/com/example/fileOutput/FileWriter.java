package com.example.fileOutput;

import java.util.ArrayList;
import java.util.Arrays;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.esotericsoftware.kryo.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Write incoming line to output file
 */
public class FileWriter extends AbstractFileOutputOperator<Long[]>
{
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);
  private static final String CHARSET_NAME = "UTF-8";
  private static final String NL = System.lineSeparator();

  @NotNull
  private String fileName;       // current file name

  private transient long id;         // operator id
  private transient String fName;    // per partition file name

  @Override
  public void setup(Context.OperatorContext context)
  {


    long startWindowId = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
    id = context.getId();
    fName = fileName + "_p" + id;
    super.setup(context);

    LOG.debug("Leaving setup, fName = {}, id = {}, startWindowId = {}", fName, id, startWindowId);
  }

  @Override
  public void beginWindow(long windowId)
  {
    LOG.debug("beginWindow: windowId = {}", windowId);
  }

  @Override
  public void processTuple(Long[] tuple)
  {
    super.processTuple(tuple);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
  }

  @Override
  protected String getFileName(Long[] tuple)
  {
    return fName;
  }

  @Override
  protected byte[] getBytesForTuple(Long[] pair)
  {
    String s = Arrays.toString(pair);
    LOG.debug("getBytesForTuple: pair = {}", s);

    byte result[] = null;
    try {
      result = (s + NL).getBytes(CHARSET_NAME);
    } catch (Exception e) {
      LOG.info("Error: got exception {}", e);
      throw new RuntimeException(e);
    }
    return result;
  }

  // getters and setters
  public String getFileName() { return fileName; }
  public void setFileName(String v) { fileName = v; }
}
