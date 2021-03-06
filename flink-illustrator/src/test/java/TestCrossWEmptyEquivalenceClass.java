import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.regex.Pattern;

public class TestCrossWEmptyEquivalenceClass {
    @Test
    public void testCrossOperationWithEmptyEquivalenceClass() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<String> input1 = env.readTextFile(Config.pathToSet1());
        DataSource<String> emptyInput2 = env.readTextFile(Config.pathToEmptyCrossSet());

        DataSet<Tuple2<Integer, String>> set1 = input1.flatMap(new OneReader());


        DataSet<Tuple2<Integer, Double>> set2 = emptyInput2.flatMap(new TwoReader());


        DataSet crossSet = set1.cross(set2);

        TupleGenerator tupleGenerator = new TupleGenerator(env,crossSet, 2);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }

    }

    public static class OneReader implements
            FlatMapFunction<String, Tuple2<Integer, String>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads Visit data-set from flat file into tuples of <User,URL>
        public void flatMap(String readLineFromFile,
                            Collector<Tuple2<Integer, String>> collector) throws Exception {

            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                int id = Integer.parseInt(tokens[0]);
                String city = tokens[1];

                collector.collect(new Tuple2<Integer, String>(id, city));
            }
        }

    }

    public static class TwoReader implements
            FlatMapFunction<String, Tuple2<Integer, Double>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads Visit data-set from flat file into tuples of <User,URL>
        public void flatMap(String readLineFromFile,
                            Collector<Tuple2<Integer, Double>> collector) throws Exception {

            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                int id = Integer.parseInt(tokens[0]);
                Double population = Double.parseDouble(tokens[1]);

                collector.collect(new Tuple2<Integer, Double>(id, population));
            }
        }

    }
}
