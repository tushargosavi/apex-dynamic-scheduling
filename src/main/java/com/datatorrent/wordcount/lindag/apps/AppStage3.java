package com.datatorrent.wordcount.lindag.apps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.lindag.operators.DefaultMonitorOperator;
import com.datatorrent.wordcount.lindag.operators.FileLineInput;
import com.datatorrent.wordcount.lindag.operators.MyGenericFileOutputOperator;
import com.datatorrent.wordcount.lindag.operators.MyUniqueCounter;

public class AppStage3 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    FileLineInput fin = dag.addOperator("Input", new FileLineInput());
    fin.setDirectory(conf.get("dt.tempOut2"));
    MyUniqueCounter counter = dag.addOperator("Counter", new MyUniqueCounter());
    MyGenericFileOutputOperator out = dag.addOperator("Output", new MyGenericFileOutputOperator());
    out.setFilePath(conf.get("dt.outputDir"));
    DefaultMonitorOperator monitor = dag.addOperator("Monitor", new DefaultMonitorOperator());
    monitor.setExpectedItems(Integer.parseInt(conf.get("partitions")));

    dag.addStream("words", fin.output, counter.data);
    dag.addStream("lower", counter.count, out.input);

    // wire in control signals
    dag.addStream("c1", fin.doneOut, counter.doneIn);
    dag.addStream("c2", counter.doneOut, out.doneIn);
    dag.addStream("c3", out.doneOut, monitor.doneIn);

  }
}
