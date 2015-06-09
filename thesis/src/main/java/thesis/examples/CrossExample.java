package thesis.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import thesis.input.datasources.InputDataSource;
import thesis.input.operatortree.OperatorTree;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class CrossExample {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        List<InputDataSource> dataSources = new ArrayList<InputDataSource>();

        DataSource<String> input1 = env.readTextFile("src/resources/CrossInput1");
        DataSource<String> input2 = env.readTextFile("src/resources/CrossInput2");

        DataSet<Tuple2<Integer, String>> set1 = input1.flatMap(new OneReader());
        InputDataSource inputDataSource1 = new InputDataSource();
        inputDataSource1.setId(0);
        inputDataSource1.setDataSet(set1);
        inputDataSource1.setName("One");

        DataSet<Tuple2<Integer, Double>> set2 = input2.flatMap(new TwoReader());
        InputDataSource inputDataSource2 = new InputDataSource();
        inputDataSource2.setId(1);
        inputDataSource2.setDataSet(set2);
        inputDataSource2.setName("Two");

        dataSources.add(inputDataSource1);
        dataSources.add(inputDataSource2);

        DataSet<Tuple4<Integer,String,Integer,Double>> crossSet = set1.cross(set2).projectFirst(0)
                .projectFirst(1).projectSecond(0).projectSecond(1);

        crossSet.print();
        OperatorTree tree = new OperatorTree(env);
        TupleGenerator tg = new TupleGenerator(dataSources,tree.createOperatorTree(),env,2);
        //env.execute();

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
