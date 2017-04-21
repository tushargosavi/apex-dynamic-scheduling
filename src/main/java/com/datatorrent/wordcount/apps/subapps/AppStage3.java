package com.datatorrent.wordcount.apps.subapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.wordcount.operators.FileLineInput;
import com.datatorrent.wordcount.operators.KeyValFileOutputOperator;

public class AppStage3 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input3", new FileLineInput());
    fin.setDirectory(conf.get("dt.tempOut2"));
    UniqueCounter<String> counter = dag.addOperator("Counter3", new UniqueCounter<String>());
    KeyValFileOutputOperator out = dag.addOperator("Output3", new KeyValFileOutputOperator());
    out.setFilePath(conf.get("dt.outputDir"));
    out.setOutputFileName("result_final");

    dag.addStream("words3", fin.output, counter.data);
    dag.addStream("counter3", counter.count, out.input);
  }
}
