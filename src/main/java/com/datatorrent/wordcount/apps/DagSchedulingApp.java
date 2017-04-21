package com.datatorrent.wordcount.apps;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.wordcount.apps.subapps.AppStage1;
import com.datatorrent.wordcount.apps.subapps.AppStage2;
import com.datatorrent.wordcount.apps.subapps.AppStage3;
import com.datatorrent.wordcount.operators.SchedulerOperator;
import com.datatorrent.wordcount.statslisteners.AppBasedLinearDAGScheduler;

@ApplicationAnnotation(name = "DagSchedulingApp")
public class DagSchedulingApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(Context.DAGContext.DEBUG, true);
    SchedulerOperator scheduler = dag.addOperator("Scheduler", new SchedulerOperator());
    dag.getMeta(scheduler).getAttributes().put(OperatorContext.STATS_LISTENERS,
        Collections.<StatsListener>singletonList(new AppBasedLinearDAGScheduler(conf, AppStage1.class, AppStage2.class, AppStage3.class)));
  }
}
