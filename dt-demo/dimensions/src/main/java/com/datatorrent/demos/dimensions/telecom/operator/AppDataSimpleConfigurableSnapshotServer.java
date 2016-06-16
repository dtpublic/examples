/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.GPOMutable;

/**
 * The tuple is a List of MutablePair<String, Long>
 * 
 * @author bright
 *
 */
public class AppDataSimpleConfigurableSnapshotServer extends AppDataConfigurableSnapshotServer<Map<String, Long>>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AppDataSimpleConfigurableSnapshotServer.class);

  @Override
  protected void convertTo(Map<String, Long> row, GPOMutable gpo)
  {
    for (Map.Entry<String, Long> entry : row.entrySet()) {
      gpo.setField(entry.getKey(), entry.getValue());
      logger.info("field: {}; value: {}", entry.getKey(), entry.getValue());
    }

  }

}
