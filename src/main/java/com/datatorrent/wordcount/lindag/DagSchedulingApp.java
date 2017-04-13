package com.datatorrent.wordcount.lindag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

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

import static org.slf4j.LoggerFactory.getLogger;

@ApplicationAnnotation(name = "DagSchedulingApp")
public class DagSchedulingApp implements StreamingApplication
{

  public static class MyScheduler extends LinearDAGScheduler
  {

    private Map<String, String> properties = new HashMap<>();
    private int index = 0;

    public MyScheduler(Configuration conf)
    {
      for (Map.Entry<String, String> entry : conf) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    private static final Logger LOG = getLogger(MyScheduler.class);

    @Override
    public boolean populateNextDAG(DAG dag)
    {
      LOG.info("Starting dag with index {}", index);
      StreamingApplication app = null;
      switch (index) {
        case 0:
          app = new AppStage1();
          break;
        case 1:
          app = new AppStage2();
          break;
        case 2:
          app = new AppStage3();
          break;
        case 3:
          return true;
        default:
          // should not reach here.
          System.exit(0);
      }

      if (app != null) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
        app.populateDAG(dag, conf);
        index++;
        return false;
      }

      return true;
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
