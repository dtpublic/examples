package com.datatorrent.tutorial.fixedwidthparser;

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


public class fixedWidthParserApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {

    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-fixedWidthParserApplication.xml"));
      conf.setLong("dt.application.fixedWidthParserApplication.operator.dataGenerator.prop.tuplesCount", 10);
      final String dataFolderPath = conf.get("dt.application.fixedWidthParserApplication.operator.*.prop.filePath");
      final String dataFileName = conf
          .get("dt.application.fixedWidthParserApplication.operator.pojoOutput.prop.outputFileName");

      FileUtils.deleteDirectory(new File(dataFolderPath));
      lma.prepareDAG(new fixedWidthParserApplication(), conf);
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
              String[] campaigns = fileData.split("\n");
              return campaigns.length == 10;
            }
          }
          return false;
        }
      });

      lc.run(30 * 1000); // runs for 30 seconds and quits

      Collection<File> files = FileUtils.listFiles(new File(dataFolderPath),
          new WildcardFileFilter(dataFileName + "*"), null);
      File paresedFile = files.iterator().next();
      String fileData = FileUtils.readFileToString(paresedFile);
      String[] campaigns = fileData.split("\n");
      int adId = 0;
      for (String actualCampaign : campaigns) {
        Assert.assertTrue(actualCampaign.contains("adId=" + adId));
        Assert.assertTrue(actualCampaign.contains("adName=" + "TestAdd"));
        Assert.assertTrue(actualCampaign.contains("bidPrice=" + "2.2"));
        Assert.assertTrue(actualCampaign.contains("parentCampaign=" + "CAMP_ad123"));
        adId++;
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
