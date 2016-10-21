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
    FileLineInput fin = dag.addOperator("Input1", new FileLineInput());
    fin.setDirectory(conf.get("dt.startDir"));
    fin.setPartitionCount(Integer.parseInt(conf.get("dt.partitions")));
    LineSplitter splitter = dag.addOperator("Splitter1", new LineSplitter());
    LineOutputOperator out = dag.addOperator("Output1", new LineOutputOperator());
    out.setFilePath(conf.get("dt.tempOut1"));
    out.setOutputFileName("result");
    DefaultMonitorOperator monitor = dag.addOperator("Monitor1", new DefaultMonitorOperator());
    monitor.setExpectedItems(Integer.parseInt(conf.get("dt.numInputFiles")));

    // dat connections.
    dag.addStream("lines", fin.output, splitter.input);
    dag.addStream("words", splitter.out, out.input);

    // wire in control tuples
    dag.addStream("c11", fin.doneOut, splitter.doneIn);
    dag.addStream("c12", splitter.doneOut, out.doneIn);
    dag.addStream("c13", out.doneOut, monitor.doneIn);
  }
}
