package com.datatorrent.wordcount.lindag.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class ControlTuplePassthroughOperator extends BaseOperator
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
