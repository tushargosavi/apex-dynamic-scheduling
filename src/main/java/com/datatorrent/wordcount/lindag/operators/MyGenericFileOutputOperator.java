package com.datatorrent.wordcount.lindag.operators;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class MyGenericFileOutputOperator extends AbstractFileOutputOperator<HashMap<String, Integer>>
{
  public transient DefaultOutputPort<String> doneOut = new DefaultOutputPort<>();
  public transient DefaultInputPort<String> doneIn = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      doneOut.emit(tuple);
    }
  };
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
