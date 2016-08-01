package com.datatorrent.wordcount;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class LineSplitter extends BaseOperator
{
  public transient DefaultOutputPort<String> words = new DefaultOutputPort<>();

  private String regex = " ";

  public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      String[] parts = tuple.split(regex);
      for (String part : parts) {
        words.emit(part);
      }
    }
  };

}
