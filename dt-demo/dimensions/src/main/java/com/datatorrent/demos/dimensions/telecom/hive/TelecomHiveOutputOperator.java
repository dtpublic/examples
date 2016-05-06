/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;
import com.datatorrent.demos.dimensions.telecom.model.BytesSupport;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.DirectoryScanner;
import com.datatorrent.lib.io.fs.AbstractSingleFileOutputOperator;

public class TelecomHiveOutputOperator<T extends BytesSupport> extends AbstractSingleFileOutputOperator<T> //AbstractFileOutputOperator<byte[]>
{
  private static final transient Logger logger = LoggerFactory.getLogger(TelecomHiveOutputOperator.class);

  public final transient DefaultOutputPort<FilePartitionMapping> hiveCmdOutput = new DefaultOutputPort<FilePartitionMapping>();
  protected static final String NON_TMP_PATTERN = "\\S+\\.\\d+$";

  public TelecomHiveOutputOperator()
  {
  }

  @Override
  protected void processTuple(T tuple)
  {
    super.processTuple(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public byte[] getBytesForTuple(T t)
  {
    return t.toBytes();
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    sendLoadDataToHiveCmd();
  }

  /**
   * send tuple to Hive to move the file to the Hive
   */
  protected final ArrayList<String> emptyPartition = Lists.newArrayList();

  protected void sendLoadDataToHiveCmd()
  {
    Set<Path> pathes = getFilePathes();
    for (Path path : pathes) {
      FilePartitionMapping mapping = new FilePartitionMapping();
      //use relative path.
      //mapping.setFilename(path.getName());
      //use absolute path
      mapping.setFilename(path.toUri().getPath());
      mapping.setPartition(emptyPartition);
      hiveCmdOutput.emit(mapping);
      logger.info("loadding data from file: {}", mapping.getFilename());
    }
    logger.debug("{} files loaded.", pathes.size());
  }

  protected transient DirectoryScanner scanner;
  protected transient Path scanPath;

  protected Set<Path> getFilePathes()
  {
    if (scanner == null) {
      scanner = buildScanner();
    }
    if (scanPath == null) {
      scanPath = new Path(filePath);
    }
    return scanner.scan(fs, scanPath, Collections.<String>emptySet());
  }

  protected transient String filePatternRegexp; // = ".*cdr\\.\\d+\\z";

  protected DirectoryScanner buildScanner()
  {
    if (filePatternRegexp == null) {
      filePatternRegexp = buildFilePatternRegexp();
    }
    if (scanner == null) {
      scanner = new DirectoryScanner();
      scanner.setFilePatternRegexp(filePatternRegexp);
    }
    return scanner;
  }

  protected String buildFilePatternRegexp()
  {
    filePatternRegexp = NON_TMP_PATTERN;
    return filePatternRegexp;
  }
}
