/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.stram.StramLocalCluster;

public class CassandraApplicationTest
{
  private static final String NODE = "localhost";
  private static final String KEYSPACE = "testapp";
  private static final String TABLE_NAME = "AppTestTable";
  private static Cluster cluster = null;
  private static Session session = null;

  private static final ArrayList<UUID> ids = new ArrayList<>();
  private static final HashMap<Integer, String> mapNames = new HashMap<>();
  private static final HashMap<Integer, Integer> mapAge = new HashMap<>();

  @BeforeClass
  public static void setup()
  {
    try {
      cluster = Cluster.builder().addContactPoint(NODE).build();
      session = cluster.connect(KEYSPACE);

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "."
          + CassandraTransactionalStore.DEFAULT_META_TABLE + " ( " + CassandraTransactionalStore.DEFAULT_APP_ID_COL
          + " TEXT, " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT, "
          + CassandraTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT, " + "PRIMARY KEY ("
          + CassandraTransactionalStore.DEFAULT_APP_ID_COL + ", " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL
          + ") " + ");";
      session.execute(createMetaTable);
      String createTable = "CREATE TABLE IF NOT EXISTS "
          + KEYSPACE
          + "."
          + TABLE_NAME
          + " (id uuid PRIMARY KEY,age int,lastname text,test boolean,floatvalue float,doubleValue double,set1 set<int>,list1 list<int>,map1 map<text,int>,last_visited timestamp);";
      session.execute(createTable);
      insertEventsInTable(10);
    } catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
    if (session != null) {
      session.execute("DROP TABLE " + CassandraTransactionalStore.DEFAULT_META_TABLE);
      session.execute("DROP TABLE " + KEYSPACE + "." + TABLE_NAME);
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  private static void insertEventsInTable(int numEvents)
  {
    try {
      Cluster cluster = Cluster.builder().addContactPoint(NODE).build();
      Session session = cluster.connect(KEYSPACE);

      String insert = "INSERT INTO " + KEYSPACE + "." + TABLE_NAME + " (ID,lastname,age)" + " VALUES (?,?,?);";
      PreparedStatement stmt = session.prepare(insert);
      BoundStatement boundStatement = new BoundStatement(stmt);
      for (int i = 0; i < numEvents; i++) {
        ids.add(UUID.randomUUID());
        mapNames.put(i, "test" + i);
        mapAge.put(i, i + 10);
        session.execute(boundStatement.bind(ids.get(i), mapNames.get(i), mapAge.get(i)));
      }
    } catch (DriverException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-CassandraInputApplication.xml"));
      conf.set("dt.operator.CassandraReader.prop.store.node", "localhost");
      conf.set("dt.operator.CassandraReader.prop.store.keyspace", KEYSPACE);
      conf.set("dt.operator.CassandraReader.prop.tablename", TABLE_NAME);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();

      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          if (getNumOfEventsInStore() == 10) {
            return true;
          }
          return false;
        }
      });
      lc.run(10000); // runs for 10 seconds and quits
      Assert.assertEquals(10, getNumOfEventsInStore());
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private long getNumOfEventsInStore()
  {
    String countQuery = "SELECT count(*) from " + KEYSPACE + "." + TABLE_NAME + ";";
    ResultSet resultSetCount = session.execute(countQuery);
    for (Row row : resultSetCount) {
      return row.getLong(0);
    }
    return 0;

  }
}
