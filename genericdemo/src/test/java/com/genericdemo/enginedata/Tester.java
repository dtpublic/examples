/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.genericdemo.enginedata;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class Tester
{
  @Test
  public void blah() throws MalformedURLException, IOException
  {
    URL schemaURL = new URL("file:/Users/ashwin/code/generic/genericdemo/target/genericdemo-1.0-SNAPSHOT.jar!/engineDataEventSchema.json");
    URL localSchemaURL = new URL("file:///tmp/blah.txt");
    FileUtils.deleteQuietly(new File(localSchemaURL.getPath()));
    FileUtils.copyFile(new File(schemaURL.getPath()), new File(localSchemaURL.getPath()));

    Path schemaPath = new Path(schemaURL.toString());
    Path localPath = new Path(localSchemaURL.toString());
    System.out.println("jar $$$ = " + schemaPath);
    System.out.println("local $$$ = " + localPath);
  }

  @Test
  public void blah2()
  {
    
  }

}
