package com.datatorrent.wordcount.extend;

import java.io.IOException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.wordcount.LineSplitter;

public class FileStatListenerSameDag implements StatsListener.StatsListenerWithContext, Serializable
{
  private static final Logger LOG = LoggerFactory.getLogger(FileStatListenerSameDag.class);
  private transient StatsListenerContext context;
  private boolean dagStarted = false;
  private boolean dagDeployed = false;
  private int counter = 0;
  private int idleWindows = 0;

  public FileStatListenerSameDag()
  {
  }

  @Override
  public Response processStats(BatchedOperatorStats stats, StatsListenerContext statsListenerContext)
  {
    context = statsListenerContext;
    return processStats(stats);
  }

  DAG.DAGChangeTransaction extendWordCountDAG()
  {
    DAG.DAGChangeTransaction dag = (DAG.DAGChangeTransaction)context.startDAGChangeTransaction();
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    UniqueCounter<String> counter = dag.addOperator("Counter", new UniqueCounter<String>());
    ConsoleOutputOperator out = dag.addOperator("Output", new ConsoleOutputOperator());
    FileReaderOperator operator = (FileReaderOperator)dag.getOperatorMeta("Reader").getOperator();
    dag.addStream("s1", operator.output, splitter.input);
    dag.addStream("s2", splitter.words, counter.data);
    dag.addStream("s3", counter.count, out.input);
    return dag;
  }

  DAG.DAGChangeTransaction undeployDag()
  {
    DAG.DAGChangeTransaction dag = context.startDAGChangeTransaction();
    for (String s : new String[]{"s1", "s2", "s3"}) {
      dag.removeStream(s);
    }
    for (String opr : new String[]{"Output", "Counter", "Splitter"}) {
      dag.removeOperator(opr);
    }
    return dag;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    if (context == null) {
      LOG.info("context is null");
    }

    String operatorName = context.getOperatorName(stats.getOperatorId());
    LOG.info("stats received for name {} id {} cwid {}", operatorName, stats.getOperatorId(), stats.getCurrentWindowId());

    if (operatorName.equals("Reader")) {
      counter++;
      for (Stats.OperatorStats ws : stats.getLastWindowedStats()) {
        Integer value = (Integer)ws.metrics.get("pendingFiles");
        LOG.info("stats received for {} pendingFiles {} counter {}", stats.getOperatorId(), value, counter);
        /** If new files are detected, and dag is not already started, attach data processing operators */
        if (value != null && value > 10 && !dagStarted && counter > 40) {
          dagStarted = true;
          context.commit(extendWordCountDAG());
          counter = 0;
          idleWindows = 0;
        }

        if (stats.getTuplesEmittedPSMA() > 0) {
          LOG.info("App started emitting ");
          dagDeployed = true;
        }

        /** IF application is idle for 120 invocation of stats listener,
         * remove the data processing operators.
         */
//        if (stats.getTuplesEmittedPSMA() == 0 && dagDeployed) {
//          idleWindows++;
//          LOG.info("Reader idle window found {}", idleWindows);
//          if (idleWindows >= 120) {
//            LOG.info("No data read for last {} windows, removing dagChanges", idleWindows);
//            try {
//              idleWindows = 0;
//              context.submitDagChange(undeployDag());
//              dagDeployed = false;
//              dagStarted = false;
//            } catch (ClassNotFoundException e) {
//              e.printStackTrace();
//            } catch (IOException e) {
//              e.printStackTrace();
//            }
//          }
//        } else {
//          idleWindows = 0;
//        }
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
      FileMonitorOperator fm = (FileMonitorOperator)operator;
      fm.handleCommand();
      return null;
    }
  }
}
