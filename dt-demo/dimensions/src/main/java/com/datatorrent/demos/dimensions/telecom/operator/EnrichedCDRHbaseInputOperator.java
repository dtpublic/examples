/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;

/**
 * This operator read Enriched
 * 
 * @author bright
 *
 */
public class EnrichedCDRHbaseInputOperator implements InputOperator
{
  private static final transient Logger logger = LoggerFactory.getLogger(EnrichedCDRHbaseInputOperator.class);

  public final transient DefaultOutputPort<EnrichedCDR> outputPort = new DefaultOutputPort<EnrichedCDR>();

  protected DataWarehouseConfig hbaseConfig = EnrichedCDRHBaseConfig.instance();
  protected HBaseStore store;
  protected int batchSize = 10;

  protected void initialize() throws IOException
  {
    //store
    store = new HBaseStore();
    store.setTableName(hbaseConfig.getTableName());
    store.setZookeeperQuorum(hbaseConfig.getHost());
    store.setZookeeperClientPort(hbaseConfig.getPort());

    store.connect();
  }

  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      initialize();
    } catch (Exception e) {
      logger.error("Initialize failed.", e);
    }
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  private ResultScanner resultScanner;

  protected ResultScanner getResultScanner() throws IOException
  {
    if (resultScanner != null) {
      return resultScanner;
    }
    HTable table = store.getTable();
    Scan scan = new Scan();
    resultScanner = table.getScanner(scan);
    return resultScanner;
  }

  /**
   * TODO: recovery mechanism
   */
  protected Map<String, byte[]> nameValueMap = new HashMap<String, byte[]>();

  @Override
  public void emitTuples()
  {
    try {
      ResultScanner scanner = getResultScanner();

      for (int i = 0; i < batchSize; ++i) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }

        nameValueMap.clear();

        //row is imsi
        nameValueMap.put("imsi", result.getRow());

        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
          String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
          byte[] value = CellUtil.cloneValue(cell);
          nameValueMap.put(columnName, value);
        }
        EnrichedCDR cdr = new EnrichedCDR(nameValueMap);

        outputPort.emit(cdr);
      }

    } catch (Exception e) {
      logger.error("emitTuples() exception", e);
    }
  }

}
