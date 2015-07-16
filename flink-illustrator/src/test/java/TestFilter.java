
import flink.examplegeneration.input.operatortree.SingleOperator;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.Test;

public class TestFilter {

    @Test
    public void testFilterOperation() throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<String> input1 = env.readTextFile(Config.pathToSet1());
        DataSource<String> input2 = env.readTextFile(Config.pathToSet2());

        DataSet<Tuple2<Integer, String>> set1 = input1.flatMap(new TestCross.OneReader());

        DataSet<Tuple2<Integer, Double>> set2 = input2.flatMap(new TestCross.TwoReader());
        DataSet<Tuple2<Integer, Double>> filterSet = set2.filter(new PopulationFilter());

        DataSet<Tuple4<Integer,String,Integer,Double>> crossSet = set1.cross(filterSet).projectFirst(0)
                .projectFirst(1).projectSecond(0).projectSecond(1);

        crossSet.printOnTaskManager("OutputSink");
        TupleGenerator tupleGenerator = new TupleGenerator(env,2);

        for(SingleOperator operator : tupleGenerator.getOperatorTree()){
            TestCase.assertNotNull(operator.getOperatorOutputAsList());
        }

    }

    public static class PopulationFilter
            implements
            FilterFunction<Tuple2<Integer, Double>> {


        public boolean filter(Tuple2<Integer, Double> value) throws Exception {
            return value.f1 > 40;
        }
    }
}
