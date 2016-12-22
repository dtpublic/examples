package com.datatorrent.tutorial.xmlparser;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

public class xmlParserApplicationTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-xmlParseApplication.xml"));
      conf.setLong("dt.application.xmlParserApplication.operator.dataGenerator.prop.tupleCount", 10);
      final String dataFolderPath = conf.get("dt.application.xmlParserApplication.operator.*.prop.filePath");
      final String dataFileName = conf
          .get("dt.application.xmlParserApplication.operator.dataOutput.prop.outputFileName");
      final String pojoFileName = conf
        .get("dt.application.xmlParserApplication.operator.pojoOutput.prop.outputFileName");
      FileUtils.deleteDirectory(new File(dataFolderPath));
      lma.prepareDAG(new com.datatorrent.tutorial.xmlparser.xmlParserApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          if (new File(dataFolderPath).exists()) {
            Collection<File> files = FileUtils.listFiles(new File(dataFolderPath), new WildcardFileFilter(dataFileName
                + "*"), null);
            if(files.size() >= 1) {
              File parsedFile = files.iterator().next();
              String fileData = FileUtils.readFileToString(parsedFile);
              String[] employees = fileData.split("\n");
              if(employees.length == 10) {
                Collection<File> ofiles = FileUtils.listFiles(new File(dataFolderPath), new WildcardFileFilter(pojoFileName
                  + "*"), null);
                if(ofiles.size() >= 1) {
                  File pojoFile = ofiles.iterator().next();
                  String pojoData = FileUtils.readFileToString(pojoFile);
                  String[] pojos = pojoData.split("\n");
                  if(pojos.length == 10) {
                    return true;
                  }
                }
              }
            }
          }
          return false;
        }
      });
      lc.run(30 * 1000); // runs for 30 seconds and quits
      Collection<File> files = FileUtils.listFiles(new File(dataFolderPath),
          new WildcardFileFilter(dataFileName + "*"), null);
      File parsedFile = files.iterator().next();
      String fileData = FileUtils.readFileToString(parsedFile);
      String[] employees = fileData.split("\n");
      int eId = 0;
      for (String actualEmployee : employees) {
        if(actualEmployee != " ") {
          Assert.assertTrue(actualEmployee.contains("eid = " + eId));
          Assert.assertTrue(actualEmployee.contains("name = employee" + eId));
          Assert.assertTrue(actualEmployee.contains("dept = department" + eId));
          eId++;
        }
      }
      //Check POJO information
      Collection<File> ofiles = FileUtils.listFiles(new File(dataFolderPath), new WildcardFileFilter(pojoFileName
        + "*"), null);
      File pojoFile = ofiles.iterator().next();
      String pojoData = FileUtils.readFileToString(pojoFile);
      String[] pojos = pojoData.split("\n");
      eId =0;
      for (String pojo : pojos) {
        if(pojo != " ") {
          Assert.assertTrue(pojo.contains("eid=" + eId));
          Assert.assertTrue(pojo.contains("name=employee" + eId));
          Assert.assertTrue(pojo.contains("dept=department" + eId));
          eId++;
        }
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
