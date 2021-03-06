package com.datatorrent.wordcount.statslisteners;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import static org.slf4j.LoggerFactory.getLogger;

public class AppBasedLinearDAGScheduler extends LinearDAGScheduler
{

  private final Class<? extends StreamingApplication>[] applications;
  private Map<String, String> properties = new HashMap<>();
  private int index = 0;

  public AppBasedLinearDAGScheduler(Configuration conf, Class<? extends StreamingApplication> ... klass)
  {
    if (conf == null) {
      conf = new Configuration();
    }
    for (Map.Entry<String, String> entry : conf) {
      properties.put(entry.getKey(), entry.getValue());
    }
    applications = klass;
  }

  private static final Logger LOG = getLogger(AppBasedLinearDAGScheduler.class);

  @Override
  public boolean populateNextDAG(DAG dag)
  {
    LOG.info("Starting dag with index {}", index);
    if (index >= applications.length) {
      return true;
    }
    StreamingApplication app;
    try {
      app = applications[index].newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      // force application stop.
      return true;
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
