package com.datatorrent.wordcount.lindag.apps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.lindag.operators.DefaultMonitorOperator;
import com.datatorrent.wordcount.lindag.operators.FileLineInput;
import com.datatorrent.wordcount.lindag.operators.LineOutputOperator;
import com.datatorrent.wordcount.lindag.operators.WordModifier;

public class AppStage2 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input2", new FileLineInput());
    fin.setDirectory(conf.get("dt.tempOut1"));
    WordModifier modifier = dag.addOperator("Modifier2", new WordModifier());
    LineOutputOperator out = dag.addOperator("Output2", new LineOutputOperator());
    out.setFilePath(conf.get("dt.tempOut2"));
    out.setOutputFileName("result1");
    DefaultMonitorOperator monitor = dag.addOperator("Monitor2", new DefaultMonitorOperator());
    monitor.setExpectedItems(Integer.parseInt(conf.get("dt.partitions")));

    // data connections.
    dag.addStream("words2", fin.output, modifier.input);
    dag.addStream("lower2", modifier.out, out.input);

    // control signals to determinte the EOD (End of DAG)
    dag.addStream("c21", fin.doneOut, modifier.doneIn);
    dag.addStream("c22", modifier.doneOut, out.doneIn);
    dag.addStream("c23", out.doneOut, monitor.doneIn);
  }
}
