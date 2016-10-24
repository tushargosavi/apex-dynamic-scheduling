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

    FileLineInput fin = dag.addOperator("Input3", new FileLineInput());
    fin.setDirectory(conf.get("dt.tempOut2"));
    MyUniqueCounter counter = dag.addOperator("Counter3", new MyUniqueCounter());
    MyGenericFileOutputOperator out = dag.addOperator("Output3", new MyGenericFileOutputOperator());
    out.setFilePath(conf.get("dt.outputDir"));
    out.setOutputFileName("result_final");
    DefaultMonitorOperator monitor = dag.addOperator("Monitor3", new DefaultMonitorOperator());
    monitor.setExpectedItems(Integer.parseInt(conf.get("dt.partitions")));

    dag.addStream("words3", fin.output, counter.data);
    dag.addStream("counter3", counter.count, out.input);

    // wire in control signals
    dag.addStream("c31", fin.doneOut, counter.doneIn);
    dag.addStream("c32", counter.doneOut, out.doneIn);
    dag.addStream("c33", out.doneOut, monitor.doneIn);

  }
}
