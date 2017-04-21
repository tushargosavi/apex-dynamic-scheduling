package com.datatorrent.wordcount.operators;

import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class SchedulerOperator extends BaseOperator implements InputOperator
{
  @Override
  public void emitTuples()
  {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // pass
    }
  }
}
