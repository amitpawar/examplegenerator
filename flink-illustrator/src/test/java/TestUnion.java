
import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.regex.Pattern;

public class TestUnion {

    @Test
    public void testUnionOperation() throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        DataSource<String> visits = env.readTextFile(Config.pathToVisits());
        DataSource<String> visitsEU = env.readTextFile(Config.pathToVisitsEU());

        DataSet<Tuple3<String, String, Integer>> visitSet = visits.flatMap(
                new VisitsReaderWithCount());


        DataSet<Tuple3<String, String, Integer>> visitEUSet = visitsEU.flatMap(new VisitsReaderWithCount());


        DataSet<Tuple3<String, String, Integer>> visitsUnion = visitSet.union(visitEUSet);

        visitsUnion.print();

        TupleGenerator tupleGenerator = new TupleGenerator(env,2);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }
    }

    public static class VisitsReaderWithCount implements
            FlatMapFunction<String, Tuple3<String, String, Integer>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads Visit data-set from flat file into tuples of <User,URL>
        public void flatMap(String readLineFromFile,
                            Collector<Tuple3<String, String, Integer>> collector) throws Exception {

            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                String user = tokens[0];
                String url = tokens[1];

                collector.collect(new Tuple3<String, String, Integer>(user, url, 1));
            }
        }
    }
}
