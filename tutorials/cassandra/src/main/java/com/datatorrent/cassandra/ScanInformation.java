/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

public class ScanInformation
{
  private int numRows;
  private int hashCode;

  private ScanInformation()
  {

  }

  public ScanInformation(int numRows, int hashCode)
  {
    this.numRows = numRows;
    this.hashCode = hashCode;
  }

  public int getHashCode()
  {
    return hashCode;
  }

  public void setHashCode(int hashCode)
  {
    this.hashCode = hashCode;
  }

  public int getNumRows()
  {
    return numRows;
  }

  public void setNumRows(int numRows)
  {
    this.numRows = numRows;
  }

}
