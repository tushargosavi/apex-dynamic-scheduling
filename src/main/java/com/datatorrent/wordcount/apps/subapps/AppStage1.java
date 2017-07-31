package com.datatorrent.wordcount.apps.subapps;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.wordcount.operators.FileLineInput;
import com.datatorrent.wordcount.operators.LineSplitter;

public class AppStage1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInput fin = dag.addOperator("Input1", new FileLineInput());
    fin.setDirectory(conf.get("dt.startDir", "/user/tushar/sim/data"));
    fin.setPartitionCount(Integer.parseInt(conf.get("dt.partitions", "1")));
    LineSplitter splitter = dag.addOperator("Splitter1", new LineSplitter());
    GenericFileOutputOperator.StringFileOutputOperator out = dag.addOperator("Output1", new GenericFileOutputOperator.StringFileOutputOperator());
    out.setFilePath(conf.get("dt.tempOut1", "/user/tushar/sim/tempOut1"));
    out.setOutputFileName("result");

    // dat connections.
    dag.addStream("lines1", fin.output, splitter.input);
    dag.addStream("words1", splitter.out, out.input);
  }
}
