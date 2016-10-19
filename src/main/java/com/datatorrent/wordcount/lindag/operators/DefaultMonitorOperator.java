package com.datatorrent.wordcount.lindag.operators;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class DefaultMonitorOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private int doneItems = 0;
  private int expectedItems = 1;
  private long currentWindowId;
  private long doneWindowId = 0;
  public transient DefaultInputPort<String> doneIn = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      doneItems++;
      if (doneItems >= expectedItems) {
        doneWindowId = currentWindowId;
      }
    }
  };
  @AutoMetric
  private Boolean done = false;

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

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
  public void checkpointed(long windowId)
  {

  }

  /**
   * If the committed is received then we know that all the operatortions are
   * performed by the DAG. we can issue an shutdown signal to terminate the
   * app.
   *
   * @param windowId
   */
  @Override
  public void committed(long windowId)
  {
    if (doneWindowId != 0 && windowId > doneWindowId) {
      done = true;
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {

  }
}
