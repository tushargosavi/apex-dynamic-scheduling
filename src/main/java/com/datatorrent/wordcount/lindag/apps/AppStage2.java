package com.datatorrent.wordcount.lindag.apps;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.lindag.operators.FileLineInput;
import com.datatorrent.wordcount.lindag.operators.WordModifier;

public class AppStage2 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input", new FileLineInput());
    WordModifier modifier = dag.addOperator("Modifier", new WordModifier());
    StringFileOutputOperator out = dag.addOperator("Output", new StringFileOutputOperator());

    dag.addStream("words", fin.output, modifier.input);
    dag.addStream("lower", modifier.out, out.input);

  }
}
