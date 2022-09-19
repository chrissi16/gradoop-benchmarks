/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmarks.tpgm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.MaxDegreeEvolution;
import org.gradoop.temporal.model.impl.operators.metric.TemporalVertexDegree;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated program to benchmark the temporal degree operator. The benchmark is expected to be executed on
 * a temporal graph dataset.
 */
public class MaxDegreeBenchmark extends BaseTpgmBenchmark {

  /**
   * Option to declare the degree type (in, out or both).
   */
  private static final String OPTION_DEGREE_TYPE = "d";
  /**
   * Option to declare the time dimension.
   */
  private static final String OPTION_TIME_DIMENSION = "t";
  /**
   * The degree type (IN, OUT or BOTH).
   */
  private static String DEGREE_TYPE;
  /**
   * The time dimension to consider (VALID_TIME or TRANSACTION_TIME)
   */
  private static String TIME_DIMENSION;

  static {
    OPTIONS.addRequiredOption(OPTION_DEGREE_TYPE, "degreeType", true, "The degree type (IN, OUT or BOTH).");
    OPTIONS.addRequiredOption(OPTION_TIME_DIMENSION, "dimension", true,
          "The time dimension (VALID_TIME or TRANSACTION_TIME)");
  }

  /**
   * Main program to run the benchmark.
   * <p>
   * Example: {@code $ /path/to/flink run -c org.gradoop.benchmarks.tpgm.MaxDegreeBenchmark
   * /path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv -d BOTH -t VALID_TIME}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TemporalDegreeBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read cmd arguments
    readBaseCMDArguments(cmd);
    DEGREE_TYPE = cmd.getOptionValue(OPTION_DEGREE_TYPE);
    TIME_DIMENSION = cmd.getOptionValue(OPTION_TIME_DIMENSION);

    // parse arguments
    VertexDegree degreeType = VertexDegree.valueOf(DEGREE_TYPE);
    TimeDimension timeDimension = TimeDimension.valueOf(TIME_DIMENSION);

    // read graph
    TemporalGraph temporalGraph = readTemporalGraph(INPUT_PATH, INPUT_FORMAT);

    TemporalGradoopConfig conf = temporalGraph.getConfig();
    ExecutionEnvironment env = conf.getExecutionEnvironment();

    // init the operator
    MaxDegreeEvolution operator = new MaxDegreeEvolution(degreeType, timeDimension);

    // apply operator
    DataSet<Tuple2<Long, Integer>> results = temporalGraph.callForValue(operator);

    // write results
    results.writeAsCsv(OUTPUT_PATH, FileSystem.WriteMode.OVERWRITE);

    env.execute(TemporalDegreeBenchmark.class.getSimpleName()+ " (" + degreeType + "," + timeDimension + "," + ") - P: " + env.getParallelism());

    writeCSV(env);
  }

  /**
   * Method to create and add lines to a csv-file for result runtime tracking.
   *
   * @param env the execution environment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String.format("%s|%s|%s|%s|%s", "Parallelism", "dataset", "degreeType", "timeDimension",
          "Runtime(s)");
    String tail = String.format("%s|%s|%s|%s|%s", env.getParallelism(), INPUT_PATH, DEGREE_TYPE,
          TIME_DIMENSION, env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));
    writeToCSVFile(head, tail);
  }
}
