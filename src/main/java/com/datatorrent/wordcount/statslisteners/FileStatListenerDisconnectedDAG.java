package com.datatorrent.wordcount.statslisteners;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.wordcount.operators.LineSplitter;

public class FileStatListenerDisconnectedDAG implements StatsListener.StatsListenerWithContext, Serializable
{
  private static final Logger LOG = LoggerFactory.getLogger(FileStatListenerDisconnectedDAG.class);
  private transient StatsListenerContext context;
  private boolean dagStarted = false;
  private int counter = 0;
  private int idleWindows = 0;

  public FileStatListenerDisconnectedDAG()
  {
  }

  @Override
  public Response processStats(BatchedOperatorStats stats, StatsListenerContext statsListenerContext)
  {
    context = statsListenerContext;
    return processStats(stats);
  }

  DAG getWordCountDag()
  {
    DAG dag = context.startDAGChangeTransaction();
    LineByLineFileInputOperator reader = dag.addOperator("Reader", new LineByLineFileInputOperator());
    List<StatsListener> listeners = new ArrayList<>();
    listeners.add(this);
    dag.getMeta(reader).getAttributes().put(Context.OperatorContext.STATS_LISTENERS, listeners);
    reader.setDirectory("/user/tushar/data");
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    UniqueCounter<String> counter = dag.addOperator("Counter", new UniqueCounter<String>());
    ConsoleOutputOperator out = dag.addOperator("Output", new ConsoleOutputOperator());
    dag.addStream("s1", reader.output, splitter.input);
    dag.addStream("s2", splitter.out, counter.data);
    dag.addStream("s3", counter.count, out.input);
    return dag;
  }

  DAG undeployDag()
  {
    DAG dag = context.startDAGChangeTransaction();
    dag.removeStream("s3");
    dag.removeOperator("Output");
    dag.removeStream("s2");
    dag.removeOperator("Counter");
    dag.removeStream("s1");
    dag.removeOperator("Splitter");
    dag.removeOperator("Reader");
    return dag;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    String name = context.getOperatorName(stats.getOperatorId());

    /** If operator is removed dynamically from DAG while stat listener is being called
     * on old accumulated stats.
     */
    if (name == null) {
      return null;
    }

    LOG.info("stats received for name {} id {} cwid {}", name, stats.getOperatorId(), stats.getCurrentWindowId());

    if (name.equals("Monitor")) {
      counter++;
      for (Stats.OperatorStats ws : stats.getLastWindowedStats()) {
        Integer value = (Integer)ws.metrics.get("pendingFiles");
        LOG.info("stats received for {} pendingFiles {} counter {}", stats.getOperatorId(), value, counter);
        if (value != null && value > 20 && !dagStarted && counter > 40) {
          try {
            dagStarted = true;
            context.commit(getWordCountDag());
            counter = 0;
            idleWindows = 0;
          } catch (Exception ex) {
            LOG.error("Exception occurred while changing dag");
          }
        }
      }
    }

    if (name.equals("Reader")) {
      if (stats.getTuplesEmittedPSMA() == 0) {
        idleWindows++;
        LOG.info("Reader idle window found {}", idleWindows);
        if (idleWindows >= 120) {
          LOG.info("No data read for last {} windows, removing dagChanges", idleWindows);
          idleWindows = 0;
          context.commit(undeployDag());
        }
      } else {
        idleWindows = 0;
      }
    }
    return null;
  }
}
