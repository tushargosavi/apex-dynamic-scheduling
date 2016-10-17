package com.datatorrent.wordcount.lindag.operators;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class DefaultMonitorOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private int doneItems = 0;
  private int expectedItems = 1;

  public transient DefaultInputPort<String> doneIn = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {

    }
  };

  @AutoMetric
  private Boolean done = false;

  public Boolean getDone()
  {
    return done;
  }

  public void setDone(Boolean done)
  {
    this.done = done;
  }

  public int getExpectedItems()
  {
    return expectedItems;
  }

  public void setExpectedItems(int expectedItems)
  {
    this.expectedItems = expectedItems;
  }

  @Override
  public void endWindow()
  {
    done = doneItems > expectedItems;
    super.endWindow();
  }

  @Override
  public void checkpointed(long windowId)
  {

  }

  @Override
  public void committed(long windowId)
  {

  }

  @Override
  public void beforeCheckpoint(long windowId)
  {

  }
}
