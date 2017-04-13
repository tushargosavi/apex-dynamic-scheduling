package com.datatorrent.wordcount.lindag.operators;

import java.io.IOException;
import java.io.InputStream;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;

public class FileLineInput extends LineByLineFileInputOperator
{
  public transient DefaultOutputPort<String> doneOut = new DefaultOutputPort<>();
  String fileName;
  boolean directoryScanned = false;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    fileName = path.getName();
    return super.openFile(path);
  }


  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    doneOut.emit(fileName);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (directoryScanned && pendingFileCount.longValue() == 0) {
      throw new ShutdownException();
    }
  }

  @Override
  protected void scanDirectory()
  {
    super.scanDirectory();
    directoryScanned = true;
  }
}
