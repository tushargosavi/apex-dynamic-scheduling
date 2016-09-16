package com.datatorrent.wordcount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.stram.StreamingContainerParent;

public class FileReaderOperator extends LineByLineFileInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerParent.class);

  @AutoMetric
  private int pendingFiles;
  private transient Context.OperatorContext context;

  @Override
  public void emitTuples()
  {
    if (output.isConnected()) {
      super.emitTuples();
    }
    super.scanDirectory();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    this.context = context;
  }

  @Override
  public void beginWindow(long windowId)
  {
    LOG.info("begin window called {} hex {}", windowId, Long.toHexString(windowId));
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    pendingFiles = pendingFileCount.intValue();
  }

  public int getPendingFiles()
  {
    return pendingFiles;
  }

  public void setPendingFiles(int pendingFiles)
  {
    this.pendingFiles = pendingFiles;
  }

}
