package com.datatorrent.wordcount.lindag;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import javax.validation.ConstraintViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import jline.internal.Log;

/**
 * This stat listener is set on the scheduler opeartor and monitor operator.
 */
public abstract class LinearDAGScheduler implements StatsListener, StatsListener.ContextAwareStatsListener, Serializable
{
  transient StatsListenerContext context;
  long lastCheckTime;
  long checkInterval = 30 * 1000; // 30 seconds.
  boolean scheduleNextDag = true;
  private transient FutureTask<Object> future;
  private transient Map<Integer, List<BatchedOperatorStats>> pendingStats = Maps.newHashMap();
  private int currentDagId = 0;
  private DAG.DAGChangeSet currentPendingDAG;
  private DAG.DAGChangeSet currentDAG;

  public abstract DAG.DAGChangeSet getNextDAG(int i, DAG.DAGChangeSet dag);

  private void addPendingStats(BatchedOperatorStats stats)
  {
    Integer id = stats.getOperatorId();
    List<BatchedOperatorStats> oldStats = pendingStats.get(id);
    if (oldStats == null) {
      oldStats = new ArrayList<>();
      pendingStats.put(id, oldStats);
    }
    oldStats.add(stats);
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {

    /** Do not process any stats while dag change is pending */
    if (future != null) {
      if (!future.isDone()) {
        addPendingStats(stats);
        return null;
      }

      try {
        Object ret = future.get();
        currentDAG = currentPendingDAG;
        currentPendingDAG = null;
        currentDagId++;
        scheduleNextDag = false;
        future = null;
      } catch (ExecutionException e) {
        handleDagChangeException(e);
      } catch (InterruptedException e) {
        handleDagChangeException(e);
      }
      return null;
    }

    /** start the initial dag */
    if (scheduleNextDag) {
      startNextDag();
      return null;
    }

    // normal processing of events.
    addPendingStats(stats);
    return processPendingStats(stats.getOperatorId());
  }

  private Response processPendingStats(int id)
  {
    List<BatchedOperatorStats> statList = pendingStats.get(id);
    if (statList == null) {
      return null;
    }

    Iterator<BatchedOperatorStats> iter = statList.iterator();
    while (iter.hasNext()) {
      BatchedOperatorStats stats = iter.next();
      Response ret = processOperatorStats(stats);
      iter.remove();
      if (ret != null) {
        return ret;
      }
    }

    return null;
  }

  private void monitorDagFinished()
  {
    long now = System.currentTimeMillis();
    if ((now - lastCheckTime) > checkInterval) {
      //if (context.getOperatorsCount() <= 1) {
      scheduleNextDag = true;
      //}
    }
  }

  private Response processOperatorStats(BatchedOperatorStats stats)
  {
    List<Stats.OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
    for (Stats.OperatorStats oStats : lastWindowedStats) {
      if (context.getOperatorName(stats.getOperatorId()).startsWith("Monitor")) {
        Boolean needsShutDown = (Boolean)oStats.metrics.get("done");
        if (needsShutDown != null && needsShutDown == true) {
          LOG.info("scheduler dag {} finished", currentDagId);
          scheduleNextDag = true;
        }
      }
    }
    return null;
  }

  /**
   * Add a stats listener on the monitor operator in the DAG, if it exists.
   */
  private void updateDAG()
  {
    LogicalPlan dag = (LogicalPlan)currentPendingDAG;
    for (DAG.OperatorMeta ometa : dag.getAllOperators()) {
      if (ometa.getName().startsWith("Monitor")) {
        LOG.info("Found monitor operator updating its stats listeners");
        Collection<StatsListener> oldListeners = ometa.getAttributes().get(OperatorContext.STATS_LISTENERS);
        if (oldListeners == null) {
          oldListeners = new ArrayList<>();
        }
        oldListeners.add(this);
        ometa.getAttributes().put(OperatorContext.STATS_LISTENERS, oldListeners);
      }
    }
  }

  protected DAG.DAGChangeSet getUndeployDag(DAG.DAGChangeSet idag)
  {
    LOG.info("preparing undeploy dag for {}", idag);
    LogicalPlan dag = (LogicalPlan)idag;
    DAG.DAGChangeSet undeployDag = context.createDAG();

    // add instruction to remove all streams
    for (DAG.StreamMeta smeta : dag.getAllStreams()) {
      undeployDag.removeStream(smeta.getName());
    }

    // add instructions to remove all operators
    for (DAG.OperatorMeta ometa : dag.getAllOperators()) {
      undeployDag.removeOperator(ometa.getName());
    }
    return undeployDag;
  }

  protected void startNextDag()
  {
    LOG.info("scheduling dag {}", currentDagId);
    DAG.DAGChangeSet undeployDag = null;
    //if (context.getOperatorsCount() > 1) {
    if (currentDagId != 0) {
      undeployDag = getUndeployDag(currentDAG);
    } else {
      undeployDag = context.createDAG();
    }
    //}

    currentPendingDAG = getNextDAG(currentDagId, undeployDag);
    updateDAG();
    try {
      future = context.submitDagChange(currentPendingDAG);
      LOG.info("submitted dag {} to engine dag {}", currentDagId, currentPendingDAG);
    } catch (IOException | ConstraintViolationException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public StatsListenerContext getContext()
  {
    return context;
  }

  @Override
  public void setContext(StatsListenerContext context)
  {
    this.context = context;
  }

  private void handleDagChangeException(Exception e)
  {
    Log.error("handing error");
  }

  private static final Logger LOG = LoggerFactory.getLogger(LinearDAGScheduler.class);

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    pendingStats = Maps.newHashMap();
  }

}
