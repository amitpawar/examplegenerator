
import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.*;

import java.util.regex.Pattern;


public class TestJoin {

    @Test
    public void testJoinOperation() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource visitSource = env.readTextFile(Config.pathToVisits());
        DataSource urlSource = env.readTextFile(Config.pathToUrls());

        DataSet visitSet = visitSource.flatMap(new VisitsReader()).distinct();
        DataSet urlSet = urlSource.flatMap(new URLsReader()).distinct();

        DataSet joinSet = visitSet.join(urlSet).where(1).equalTo(0);

        joinSet.print();

        TupleGenerator tupleGenerator = new TupleGenerator(env.getConfig(),env.createProgramPlan(),4);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }


    }

    public static class VisitsReader implements
            FlatMapFunction<String, Tuple2<String, String>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads Visit data-set from flat file into tuples of <User,URL>
        public void flatMap(String readLineFromFile,
                            Collector<Tuple2<String, String>> collector) throws Exception {

            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                String user = tokens[0];
                String url = tokens[1];

                collector.collect(new Tuple2<String, String>(user, url));
            }
        }

    }

    public static class URLsReader implements
            FlatMapFunction<String, Tuple2<String, Long>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads URL data-set from flat file into tuples of <URL,PageRank>
        public void flatMap(String readLineFromFile,
                            Collector<Tuple2<String, Long>> collector) throws Exception {

            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                String url = tokens[0];
                Long pageRank = Long.parseLong(tokens[1]);

                collector.collect(new Tuple2<String, Long>(url, pageRank));
            }
        }

    }


}
