package com.datatorrent.wordcount;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ExtendApp")
public class ExtendApp implements StreamingApplication
{
  private FileReaderOperator reader;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    reader = dag.addOperator("Reader", new FileReaderOperator());
    reader.setDirectory("/user/hadoop/data");
    dag.getMeta(reader).getAttributes().put(Context.OperatorContext.STATS_LISTENERS, Arrays.<StatsListener>asList(new FileStatListenerSameDag()));
  }
}
