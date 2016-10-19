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
    FileLineInput fin = dag.addOperator("Input", new FileLineInput());
    fin.setDirectory(conf.get("dt.tempOut1"));
    WordModifier modifier = dag.addOperator("Modifier", new WordModifier());
    LineOutputOperator out = dag.addOperator("Output", new LineOutputOperator());
    out.setFilePath(conf.get("dt.tempOut2"));

    DefaultMonitorOperator monitor = dag.addOperator("Monitor", new DefaultMonitorOperator());
    monitor.setExpectedItems(Integer.parseInt(conf.get("partitions")));
    // data connections.
    dag.addStream("words", fin.output, modifier.input);
    dag.addStream("lower", modifier.out, out.input);

    // control signals to determinte the EOD (End of DAG)
    dag.addStream("control1", fin.doneOut, modifier.doneIn);
    dag.addStream("control2", modifier.doneOut, out.doneIn);
    dag.addStream("control3", out.doneOut, monitor.doneIn);
  }
}
