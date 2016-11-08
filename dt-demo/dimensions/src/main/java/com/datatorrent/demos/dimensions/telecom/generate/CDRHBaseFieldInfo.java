/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import com.datatorrent.contrib.hbase.HBaseFieldInfo;

public class CDRHBaseFieldInfo extends HBaseFieldInfo
{
  public CDRHBaseFieldInfo(String columnName, String columnExpression, SupportType type, String familyName)
  {
    super(columnName, columnExpression, type, familyName);
  }

  /**
   * get rid of the null point exception. should be fix it in
   * HBasePOJOPutOperator or HBaseFieldInfo
   */
  @Override
  public byte[] toBytes(Object value)
  {
    if (value == null) {
      return null;
    }
    return super.toBytes(value);
  }
}
