package com.datatorrent.wordcount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StreamingContainerParent;

public class LineSplitter extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerParent.class);

  public transient DefaultOutputPort<String> words = new DefaultOutputPort<>();

  private String regex = " ";

  private transient boolean dumpStack = true;

  public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      String[] parts = tuple.split(regex);
      for (String part : parts) {
        words.emit(part);
      }
      if (dumpStack) {
        dumpStack = false;
        Thread.dumpStack();
      }
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    LOG.info("begin window called {} hex {}", windowId, Long.toHexString(windowId));
    if (dumpStack) {
      Thread.dumpStack();
    }
  }
}
