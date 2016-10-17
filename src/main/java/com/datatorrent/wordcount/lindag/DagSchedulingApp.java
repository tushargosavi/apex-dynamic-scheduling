package com.datatorrent.wordcount.lindag;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.engine.OperatorContext;

public class DagSchedulingApp implements StreamingApplication
{

  public static class MyScheduler extends LinearDAGScheduler
  {
    public MyScheduler(Configuration conf)
    {
      this.conf = conf;
    }

    private Configuration conf;

    @Override
    public DAG.DAGChangeSet getNextDAG(int i, DAG.DAGChangeSet dag)
    {
      StreamingApplication app = null;
      switch (i) {
        case 0 :
          app = new AppStage1();
          break;
        case 1:
          app = new AppStage2();
          break;
        case 2 :
          app = new AppStage3();
          break;
      }

      if (app != null)
        app.populateDAG(dag, conf);

      return dag;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SchedulerOperator scheduler = dag.addOperator("Scheduler", new SchedulerOperator());
    dag.getMeta(scheduler).getAttributes().put(OperatorContext.STATS_LISTENERS,
      Collections.<StatsListener>singletonList(new MyScheduler(conf)));
  }
}
