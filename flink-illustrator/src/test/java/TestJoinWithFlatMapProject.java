import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class TestJoinWithFlatMapProject {

    @Test
    public void testJoinOperationWithFlatMapProjection() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource visitSource = env.readTextFile(Config.pathToVisits());
        DataSource urlSource = env.readTextFile(Config.pathToUrls());

        DataSet visitSet = visitSource.flatMap(new TestJoin.VisitsReader()).distinct();
        DataSet urlSet = urlSource.flatMap(new TestJoin.URLsReader()).distinct();

        DataSet joinSet = visitSet.join(urlSet).where(1).equalTo(0);

        DataSet<Tuple3<String, String, Long>> printSet = joinSet.flatMap(new PrintResult());

        DataSet projectSet = printSet.project(1,2);

        projectSet.printOnTaskManager("OutputSink");

        TupleGenerator tupleGenerator = new TupleGenerator(env,4);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }
    }

    public static class PrintResult
            implements
            FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple3<String, String, Long>> {

        // Merges the tuples to create tuples of <User,URL,PageRank>
        public void flatMap(
                Tuple2<Tuple2<String, String>, Tuple2<String, Long>> joinSet,
                Collector<Tuple3<String, String, Long>> collector)
                throws Exception {

            collector.collect(new Tuple3<String, String, Long>(joinSet.f0.f0,
                    joinSet.f0.f1, joinSet.f1.f1));
        }

    }
}
