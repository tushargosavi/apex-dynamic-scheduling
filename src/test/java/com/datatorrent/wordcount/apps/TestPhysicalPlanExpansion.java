package com.datatorrent.wordcount.apps;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.stram.plan.logical.DAGChangeTransactionImpl;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.DAGChangeHandlerImpl;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.wordcount.apps.subapps.AppStage1;

public class TestPhysicalPlanExpansion
{
  static class NoOpStorageAgent implements StorageAgent
  {
    @Override
    public void save(Object o, int i, long l) throws IOException
    {

    }

    @Override
    public Object load(int i, long l) throws IOException
    {
      return null;
    }

    @Override
    public void delete(int i, long l) throws IOException
    {

    }

    @Override
    public long[] getWindowIds(int i) throws IOException
    {
      return new long[0];
    }
  }

  @Test
  public void testExpansion()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new NoOpStorageAgent());
    Configuration conf = new Configuration();

    DagSchedulingApp app = new DagSchedulingApp();
    app.populateDAG(dag, conf);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    DAGChangeTransactionImpl transaction = new DAGChangeTransactionImpl(dag);
    AppStage1 app1 = new AppStage1();
    app1.populateDAG(transaction, conf);

    DAGChangeHandlerImpl handler = new DAGChangeHandlerImpl(plan);
    transaction.commit(handler);
  }
}
