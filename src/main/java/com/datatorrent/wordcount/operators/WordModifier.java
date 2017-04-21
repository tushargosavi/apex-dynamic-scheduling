package com.datatorrent.wordcount.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class WordModifier extends BaseOperator
{
  public transient DefaultOutputPort<String> out = new DefaultOutputPort<>();

  public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      String lower = tuple.toLowerCase();
      out.emit(lower);
    }
  };
}
