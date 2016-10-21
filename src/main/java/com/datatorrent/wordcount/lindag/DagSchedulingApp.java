package com.datatorrent.wordcount.lindag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.wordcount.lindag.apps.AppStage1;
import com.datatorrent.wordcount.lindag.apps.AppStage2;
import com.datatorrent.wordcount.lindag.apps.AppStage3;

@ApplicationAnnotation(name = "DagSchedulingApp")
public class DagSchedulingApp implements StreamingApplication
{

  public static class MyScheduler extends LinearDAGScheduler
  {
    private Map<String, String> properties = new HashMap<>();

    public MyScheduler(Configuration conf)
    {
      for (Map.Entry<String, String> entry : conf) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public DAG.DAGChangeSet getNextDAG(int i, DAG.DAGChangeSet dag)
    {
      StreamingApplication app = null;
      switch (i) {
        case 0:
          app = new AppStage1();
          break;
        case 1:
          app = new AppStage2();
          break;
        case 2:
          app = new AppStage3();
          break;
        default:
          System.exit(0);
      }

      if (app != null) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
        app.populateDAG(dag, conf);
      }

      return dag;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(Context.DAGContext.DEBUG, true);
    SchedulerOperator scheduler = dag.addOperator("Scheduler", new SchedulerOperator());
    dag.getMeta(scheduler).getAttributes().put(OperatorContext.STATS_LISTENERS,
        Collections.<StatsListener>singletonList(new MyScheduler(conf)));
  }
}
