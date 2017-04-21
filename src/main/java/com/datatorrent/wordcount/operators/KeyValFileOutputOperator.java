package com.datatorrent.wordcount.operators;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class KeyValFileOutputOperator extends AbstractFileOutputOperator<HashMap<String, Integer>>
{
  private String outputFileName;

  @Override
  protected String getFileName(HashMap<String, Integer> o)
  {
    return outputFileName;
  }

  @Override
  protected byte[] getBytesForTuple(HashMap<String, Integer> o)
  {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, Integer> entry : o.entrySet()) {
      builder.append(entry.getKey());
      builder.append(":");
      builder.append(entry.getValue());
      builder.append("\n");
    }
    return builder.toString().getBytes();
  }

  public void setOutputFileName(String outputFileName)
  {
    this.outputFileName = outputFileName;
  }

  public String getOutputFileName()
  {
    return outputFileName;
  }
}
