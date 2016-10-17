package com.datatorrent.wordcount.lindag.apps;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.lindag.operators.FileLineInput;
import com.datatorrent.wordcount.lindag.operators.LineSplitter;

public class AppStage1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input", new FileLineInput());
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    StringFileOutputOperator out = dag.addOperator("Output", new StringFileOutputOperator());

    dag.addStream("lines", fin.output, splitter.input);
    dag.addStream("words", splitter.out, out.input);

    // wire in control tuples
    dag.addStream("c1", fin.done, splitter.doneIn);
    dag.addStream("c2", splitter.doneOut, out.doneIn);
  }
}
