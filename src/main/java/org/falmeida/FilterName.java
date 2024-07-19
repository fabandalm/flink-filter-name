package org.falmeida;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class FilterName {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

//        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<String> text = env.readTextFile("/Users/falmeida/env-dev/github/git-local-repo/name-list.txt");

        DataSet<String> filtered = text.filter(value -> value.startsWith("N"));

        Thread.sleep(1000);

        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[]{0}).sum(1);

//        counts.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        counts.writeAsText("/Users/falmeida/env-dev/github/git-local-repo/filtered-name-list.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Filter Name Flink Application");

    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }

}
