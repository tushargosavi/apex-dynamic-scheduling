package com.datatorrent.wordcount.lindag.operators;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class LineOutputOperator extends GenericFileOutputOperator.StringFileOutputOperator
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
}
