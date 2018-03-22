/**
 * Put your copyright and license info here.
 */
package com.example.offHeapGroupBy;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="OffheapGroupBy")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(500);

    OffHeapGroupByOperator groupByOperator = dag.addOperator("OffHeapGroupByOperator", new OffHeapGroupByOperator());
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("randomData", randomGenerator.out, groupByOperator.input);
    dag.addStream("groupBy", groupByOperator.output, console.input);
  }
}
