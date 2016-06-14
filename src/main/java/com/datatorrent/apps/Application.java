package com.datatorrent.apps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.stream.DevNullCounter;

@ApplicationAnnotation(name = "UniqeValueCountTest")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    Generator gen = dag.addOperator("Generator", new Generator());
    UniqueValueCount<Integer> counter = dag.addOperator("Counter", new UniqueValueCount<Integer>());
    dag.getMeta(counter).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 60);
    ConsoleOutputOperator output = dag.addOperator("Null", new ConsoleOutputOperator());

    dag.addStream("s1", gen.output, counter.input);
    dag.addStream("devnull", counter.output, output.input).setLocality(DAG.Locality.THREAD_LOCAL);
  }
}
