package com.datatorrent.wordcount.lindag.operators;

import java.io.IOException;
import java.io.InputStream;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;

public class FileLineInput extends LineByLineFileInputOperator
{
  String fileName;

  public transient DefaultOutputPort<String> done = new DefaultOutputPort<>();

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
    done.emit(fileName);
  }
}
