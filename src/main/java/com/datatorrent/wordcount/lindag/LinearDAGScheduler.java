package com.datatorrent.wordcount.lindag;

import java.io.Serializable;

import javax.validation.ConstraintViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;

/**
 * This stat listener is set on the scheduler opeartor and monitor operator.
 */
public abstract class LinearDAGScheduler implements StatsListener, StatsListener.StatsListenerWithContext, Serializable
{
  private int currentDagId = 0;
  private DAG.DAGChangeTransaction currentDAG = null;
  private final String SCHEDULER_OPERATOR_NAME = "Scheduler";

  @Override
  public Response processStats(BatchedOperatorStats stats, StatsListenerContext context)
  {
    /* If all operators are killed except the scheduler */
    if (context.getNumPhysicalOperators() == 1) {
      startNextDag(context);
    }

    return null;
  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    return null;
  }

  abstract boolean populateNextDAG(DAG dag);

  protected void startNextDag(StatsListenerContext context)
  {
    LOG.info("scheduling dag {}", currentDagId);
    DAG.DAGChangeTransaction dag = context.startDAGChangeTransaction();

    for (DAG.StreamMeta smeta : dag.getAllStreamsMeta()) {
      dag.removeStream(smeta.getName());
    }

      // add instructions to remove all operators
    for (DAG.OperatorMeta ometa : dag.getAllOperatorsMeta()) {
      if (!ometa.getName().equals(SCHEDULER_OPERATOR_NAME)) {
        dag.removeOperator(ometa.getName());
      }
    }

    boolean end = populateNextDAG(dag);
    if (end) {
      LOG.info("End of dag removing schedulear operator ");
      dag.removeOperator(SCHEDULER_OPERATOR_NAME);
    }
    try {
      context.commit(dag);
      LOG.info("submitted dag {} to engine dag {}", currentDagId++, dag);
    } catch (ConstraintViolationException e) {
      handleDagChangeException(e);
    }
  }

  private void handleDagChangeException(Exception e)
  {
    e.printStackTrace();
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(LinearDAGScheduler.class);
}
