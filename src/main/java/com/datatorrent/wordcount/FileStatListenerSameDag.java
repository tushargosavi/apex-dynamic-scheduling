package com.datatorrent.wordcount;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSet;

public class FileStatListenerSameDag implements StatsListener, Serializable
{
  private static final Logger LOG = LoggerFactory.getLogger(FileStatListenerSameDag.class);

  public FileStatListenerSameDag() { }

  DAGChangeSet getWordCountDag()
  {
    DAGChangeSet dag = new DAGChangeSet();
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    UniqueCounter<String> counter = dag.addOperator("Counter", new UniqueCounter<String>());
    ConsoleOutputOperator out = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("s1", "Reader", "output", splitter.input);
    dag.addStream("s2", splitter.words, counter.data);
    dag.addStream("s3", counter.count, out.input);
    return dag;
  }

  DAGChangeSet undeployDag()
  {
    DAGChangeSet dag = new DAGChangeSet();
    for (String s : new String[]{"s1", "s2", "s3"}) {
      dag.removeStream(s);
    }
    for (String opr : new String[]{ "Output", "Counter", "Splitter"}) {
      dag.removeOperator(opr);
    }
    return dag;
  }

  private boolean dagStarted = false;
  private boolean dagDeployed = false;
  private int counter = 0;
  private int idleWindows = 0;

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    LOG.info("stats received for name {} id {} cwid {}", stats.getOperatorName(), stats.getOperatorId(), stats.getCurrentWindowId());

    if (stats.getOperatorName().equals("Reader")) {
      counter++;
      for (Stats.OperatorStats ws : stats.getLastWindowedStats()) {
        Integer value = (Integer)ws.metrics.get("pendingFiles");
        LOG.info("stats received for {} pendingFiles {} counter {}", stats.getOperatorId(), value, counter);
        if (value != null && value > 10 && !dagStarted && counter > 40) {
          dagStarted = true;
          Response resp = new Response();
          resp.dag = getWordCountDag();
          counter = 0;
          idleWindows = 0;
          return resp;
        }

        if (stats.getTuplesEmittedPSMA() > 0) {
          LOG.info("App started emitting ");
          dagDeployed = true;
        }

        if (stats.getTuplesEmittedPSMA() == 0 && dagDeployed) {
          idleWindows++;
          LOG.info("Reader idle window found {}", idleWindows);
          if (idleWindows >= 120) {
            LOG.info("No data read for last {} windows, removing dag", idleWindows);
            Response resp = new Response();
            idleWindows = 0;
            resp.dag = undeployDag();
            dagDeployed = false;
            dagStarted = false;
            return resp;
          }
        } else {
          idleWindows = 0;
        }
      }
    }
    return null;
  }


  static class ResetOperatorRequest implements OperatorRequest
  {
    private static final Logger LOG = LoggerFactory.getLogger(FileStatListenerSameDag.class);

    @Override
    public OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      LOG.info("ResetOperator request called {} id {} windowId {}", operator, operatorId, windowId);
      FileMonitorOperator fm =  (FileMonitorOperator)operator;
      fm.handleCommand();
      return null;
    }
  }
}
