package com.datatorrent.wordcount.lindag.apps;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.wordcount.lindag.operators.FileLineInput;

public class AppStage3 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input", new FileLineInput());
    UniqueCounter<String> counter = dag.addOperator("Counter", new UniqueCounter<String>());
    GenericFileOutputOperator out = dag.addOperator("Output", new GenericFileOutputOperator());

    dag.addStream("words", fin.output, counter.data);
    dag.addStream("lower", counter.count, out.input);

  }
}
