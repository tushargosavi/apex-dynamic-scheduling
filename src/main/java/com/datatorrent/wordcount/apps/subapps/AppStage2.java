package com.datatorrent.wordcount.apps.subapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.operators.FileLineInput;
import com.datatorrent.wordcount.operators.LineOutputOperator;
import com.datatorrent.wordcount.operators.WordModifier;

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

    // data connections.
    dag.addStream("words2", fin.output, modifier.input);
    dag.addStream("lower2", modifier.out, out.input);
  }
}
