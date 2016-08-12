package com.datatorrent.tutorial.csvparser;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

public class csvParserApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {

    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-csvParseApplication.xml"));
      conf.setLong("dt.application.csvParseApplication.operator.dataGenerator.prop.tuplesCount", 10);
      final String dataFolderPath = conf.get("dt.application.csvParseApplication.operator.*.prop.filePath");
      final String dataFileName = conf
          .get("dt.application.csvParseApplication.operator.dataOutput.prop.outputFileName");

      FileUtils.deleteDirectory(new File(dataFolderPath));
      lma.prepareDAG(new csvParserApplication(), conf);
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
            return files.size() >= 1;
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
      int adId = 1;
      for (String actualCampaign : campaigns) {
        Assert.assertTrue(actualCampaign.contains("adId=" + adId));
        Assert.assertTrue(actualCampaign.contains("adName=" + "TestAdd"));
        Assert.assertTrue(actualCampaign.contains("campaignId=" + (adId + 10)));
        Assert.assertTrue(actualCampaign.contains("bidPrice=" + "2.2"));
        Assert.assertTrue(actualCampaign.contains("securityCode=" + (adId + 10)));
        Assert.assertTrue(actualCampaign.contains("parentCampaign=" + "CAMP_add"));
        adId++;
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
