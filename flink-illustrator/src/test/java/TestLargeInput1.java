import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Pattern;

public class TestLargeInput1 {

    @Test
    public void testReddit() throws Exception{

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> idList = randomArrayList(1000);
        DataSource<Integer> idSource = environment.fromCollection(idList);
        DataSet idSet = idSource.flatMap(new IDReader());

        DataSource redditSoruce = environment.readTextFile("/home/amit/thesis/inputs/submissions.csv");
        DataSet redditSet = redditSoruce.flatMap(new CSVReader());

        DataSet joinSet = idSet.join(redditSet).where(0).equalTo(0);

        joinSet.printOnTaskManager("JoinSet");

        TupleGenerator tupleGenerator = new TupleGenerator(environment,5);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }
    }

    public ArrayList<Integer> randomArrayList(int n)
    {
        ArrayList<Integer> list = new ArrayList();
        Random random = new Random();

        for (int i = 0; i < n; i++)
        {
            list.add(random.nextInt(10000));
        }
        return list;
    }

    public static class CSVReader implements
            FlatMapFunction<String, Tuple2<String, String>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads Visit data-set from flat file into tuples of <User,URL>
        public void flatMap(String readLineFromFile,
                            Collector<Tuple2<String, String>> collector) throws Exception {

            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                String id = tokens[0];
                String title = tokens[3];

                collector.collect(new Tuple2<String, String>(id, title));
            }
        }

    }

    public static class IDReader implements
            FlatMapFunction<Integer, Tuple1<String>> {


        public void flatMap(Integer integer, Collector<Tuple1<String>> collector) throws Exception {
            collector.collect(new Tuple1<String>(integer.toString()));
        }
    }
}
