import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

public class TestMultiOperators {

    ExecutionEnvironment env = ExecutionEnvironment
            .getExecutionEnvironment();
    @Test
    public void testUnionJoinProject()throws Exception{


        DataSource<String> visits = env.readTextFile(Config.pathToVisits());
        DataSource<String> visitsEU = env.readTextFile(Config.pathToVisitsEU());
        DataSource<String> urls = env.readTextFile(Config.pathToUrls());

        DataSet<Tuple2<String, String>> visitSet = visits.flatMap(new TestJoin.VisitsReader());
        DataSet<Tuple2<String, String>> visitEUSet = visitsEU.flatMap(new TestJoin.VisitsReader());
        DataSet<Tuple2<String, Long>> urlSet = urls.flatMap(new TestJoin.URLsReader());


        DataSet<Tuple2<String, String>> visitsUnion = visitSet.union(visitEUSet);
        DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> joinSet = visitsUnion.join(urlSet).where(1).equalTo(0);

        DataSet projectSet = joinSet.project(1);

        projectSet.printOnTaskManager("OutputSink");

        TupleGenerator tupleGenerator = new TupleGenerator(env,4);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }
    }

    @Test
    public void testUnionFilterJoin()throws Exception {

        DataSource<String> visits = env.readTextFile(Config.pathToVisits());
        DataSource<String> visitsEU = env.readTextFile(Config.pathToVisitsEU());
        DataSource<String> urls = env.readTextFile(Config.pathToUrls());

        DataSet<Tuple2<String, String>> visitSet = visits.flatMap(new TestJoin.VisitsReader());
        DataSet<Tuple2<String, String>> visitEUSet = visitsEU.flatMap(new TestJoin.VisitsReader());
        DataSet<Tuple2<String, Long>> urlSet = urls.flatMap(new TestJoin.URLsReader());

        DataSet<Tuple2<String,Long>> filteredUrls = urlSet.filter(new RankFilter());
        DataSet<Tuple2<String, String>> visitsUnion = visitSet.union(visitEUSet);

        DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> joinSet = visitsUnion.join(filteredUrls).where(1).equalTo(0);

        joinSet.printOnTaskManager("OutputSink");

        TupleGenerator tupleGenerator = new TupleGenerator(env,2);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }
    }

    @Test
    public void testUnionCross() throws Exception{

        DataSource<String> visits = env.readTextFile(Config.pathToVisits());
        DataSource<String> visitsEU = env.readTextFile(Config.pathToVisitsEU());
        DataSource<String> crossSource = env.readTextFile(Config.pathToSet2());

        DataSet<Tuple2<String, String>> visitSet = visits.flatMap(new TestJoin.VisitsReader());
        DataSet<Tuple2<String, String>> visitEUSet = visitsEU.flatMap(new TestJoin.VisitsReader());
        DataSet<Tuple2<Integer,Double>> crossSet = crossSource.flatMap(new TestCross.TwoReader());

        DataSet<Tuple2<String, String>> visitsUnion = visitSet.union(visitEUSet);

        DataSet outputSet = visitsUnion.cross(crossSet);

        outputSet.printOnTaskManager("OutputSink");

        TupleGenerator tupleGenerator = new TupleGenerator(env,2);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }
    }


    public static class RankFilter
            implements
            FilterFunction<Tuple2<String, Long>> {

        // Returns true if PageRank is greater than 2
        public boolean filter(
                Tuple2<String, Long> joinSet)
                throws Exception {

            return joinSet.f1 > 2;
        }

    }
}
