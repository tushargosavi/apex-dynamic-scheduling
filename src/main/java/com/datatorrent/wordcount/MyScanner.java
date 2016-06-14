package com.datatorrent.wordcount;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class MyScanner extends AbstractFileInputOperator.DirectoryScanner
{
  @Override
  protected boolean acceptFile(String filePathStr)
  {
    boolean ret =  super.acceptFile(filePathStr);
    System.out.println("accepting file " + filePathStr + " ret " + ret);
    return ret;
  }
}
