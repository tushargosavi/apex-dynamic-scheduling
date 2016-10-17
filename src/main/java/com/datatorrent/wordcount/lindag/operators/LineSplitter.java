package com.datatorrent.wordcount.lindag.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class LineSplitter extends ControlTuplePassthroughOperator
{
  public transient DefaultOutputPort<String> out = new DefaultOutputPort<>();
  public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      String[] parts = tuple.split(" ");
      for (String part : parts) {
        out.emit(part);
      }
    }
  };
}
