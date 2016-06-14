package com.datatorrent.wordcount;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "WordCountApp")
public class WordCountApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LineByLineFileInputOperator reader = dag.addOperator("Reader", new LineByLineFileInputOperator());
    reader.setScanner(new MyScanner());

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("s1", reader.output, console.input).setLocality(DAG.Locality.THREAD_LOCAL);
  }
}
