package com.datatorrent.wordcount.extend;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSetImpl;
import com.datatorrent.wordcount.LineSplitter;

public class FileStatListenerDisconnectedDAG implements StatsListener, StatsListener.ContextAwareStatsListener, Serializable
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
  public void setContext(StatsListenerContext context)
  {
    this.context = context;
  }

  DAGChangeSetImpl getWordCountDag()
  {
    DAGChangeSetImpl dag = (DAGChangeSetImpl)context.createDAG();
    LineByLineFileInputOperator reader = dag.addOperator("Reader", new LineByLineFileInputOperator());
    List<StatsListener> listeners = new ArrayList<>();
    listeners.add(this);
    dag.getMeta(reader).getAttributes().put(Context.OperatorContext.STATS_LISTENERS, listeners);
    reader.setDirectory("/user/hadoop/data");
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    UniqueCounter<String> counter = dag.addOperator("Counter", new UniqueCounter<String>());
    ConsoleOutputOperator out = dag.addOperator("Output", new ConsoleOutputOperator());
    dag.addStream("s1", reader.output, splitter.input);
    dag.addStream("s2", splitter.words, counter.data);
    dag.addStream("s3", counter.count, out.input);
    return dag;
  }

  DAGChangeSetImpl undeployDag()
  {
    DAGChangeSetImpl dag = new DAGChangeSetImpl();
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
        if (value != null && value > 100 && !dagStarted && counter > 40) {
          try {
            dagStarted = true;
            FutureTask<Object> o = context.submitDagChange(getWordCountDag());
            counter = 0;
            idleWindows = 0;
          } catch (Exception ex) {
            LOG.error("Exception occured while changing dag");
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
          try {
            idleWindows = 0;
            FutureTask<Object> o = context.submitDagChange(undeployDag());
          } catch (ClassNotFoundException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } else {
        idleWindows = 0;
      }
    }
    return null;
  }

  static class ResetOperatorRequest implements OperatorRequest
  {
    private static final Logger LOG = LoggerFactory.getLogger(FileStatListenerDisconnectedDAG.class);

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
