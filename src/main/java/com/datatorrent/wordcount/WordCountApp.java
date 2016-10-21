package com.datatorrent.wordcount;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.wordcount.extend.FileMonitorOperator;
import com.datatorrent.wordcount.extend.FileStatListenerDisconnectedDAG;

@ApplicationAnnotation(name = "WordCountApp")
public class WordCountApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(Context.DAGContext.DEBUG, true);
    FileMonitorOperator monitor = dag.addOperator("Monitor", new FileMonitorOperator());
    monitor.setPathStr("/user/hadoop/data");
    dag.getMeta(monitor).getAttributes().put(Context.OperatorContext.STATS_LISTENERS,
      Arrays.<StatsListener>asList(new FileStatListenerDisconnectedDAG()));
  }
}
