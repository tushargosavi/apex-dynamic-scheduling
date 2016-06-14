package com.datatorrent.apps;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class Generator extends BaseOperator implements InputOperator
{

  public transient DefaultOutputPort<KeyValPair<Integer, String>> output = new DefaultOutputPort<>();
  private String[] strs = {"1", "2", "3", "4", "5", "6"};
  private transient Random random = new Random();

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < 10; i++) {
      KeyValPair<Integer, String> tuple = new KeyValPair<Integer, String>(random.nextInt(40), strs[random.nextInt(strs.length)]);
      output.emit(tuple);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
