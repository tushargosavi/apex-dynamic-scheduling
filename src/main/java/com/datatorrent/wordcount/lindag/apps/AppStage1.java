package com.datatorrent.wordcount.lindag.apps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.lindag.operators.DefaultMonitorOperator;
import com.datatorrent.wordcount.lindag.operators.FileLineInput;
import com.datatorrent.wordcount.lindag.operators.LineOutputOperator;
import com.datatorrent.wordcount.lindag.operators.LineSplitter;

public class AppStage1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input", new FileLineInput());
    fin.setDirectory(conf.get("dt.startDir"));
    fin.setPartitionCount(Integer.parseInt(conf.get("partitions")));
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    LineOutputOperator out = dag.addOperator("Output", new LineOutputOperator());
    out.setFilePath(conf.get("dt.tempOut1"));
    DefaultMonitorOperator monitor = dag.addOperator("Monitor", new DefaultMonitorOperator());
    monitor.setExpectedItems(Integer.parseInt("numInputFiles"));

    // dat connections.
    dag.addStream("lines", fin.output, splitter.input);
    dag.addStream("words", splitter.out, out.input);

    // wire in control tuples
    dag.addStream("c1", fin.doneOut, splitter.doneIn);
    dag.addStream("c2", splitter.doneOut, out.doneIn);
    dag.addStream("c3", out.doneOut, monitor.doneIn);
  }
}
