package flink.examplegeneration.examples;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


import org.apache.flink.api.common.operators.TupleGenerator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

public class SampleTest {

	public static void main(String[] args) throws Exception {


		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		//DataSource<String> visits = env.readTextFile(Config.pathToVisits());

        DataSource<Tuple2<LongWritable, Text>> visits = env.readHadoopFile(new TextInputFormat(), LongWritable.class,
                Text.class, Config.pathToVisits());
		DataSource<String> urls = env.readTextFile(Config.pathToUrls());
		
		
		
		DataSet<Tuple2<String, String>> visitSet = visits.flatMap(
				new VisitsHadoopReader()).distinct();

		
		//DataSet<Visits> visitSet = visits.flatMap(new VisitsPOJAReader());

		DataSet<Tuple2<String, Long>> urlSet = urls.flatMap(new URLsReader()).distinct();

		/*DataSet<Tuple2<Visits, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);*/
		
		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);

		////
      /*  DataSource<String> forUnion = env.readTextFile("/home/amit/thesis/dataflow/union");
        DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> unionSet = forUnion.flatMap(new UnionReader());
        dataSets.add(unionSet);
        InputDataSource input3 = new InputDataSource();
        input3.setDataSet(unionSet);
        input3.setName("Third Source");
        input3.setId(2);

        dataSources.add(input3);

        DataSet joinPlusUnionSet = joinSet.union(unionSet);*/

        ////
		//DataSet<Tuple2<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple2<Tuple2<String, String>, Tuple2<String, Long>>>> crossSet = joinSet.cross(joinSet);

		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> filterSet = joinSet
				.filter(new RankFilter());

		DataSet<Tuple3<String, String, Long>> printSet = joinSet//.project(1);
		 .flatMap(new PrintResult());

		//crossSet.print();
		printSet.print();  //datasink needed

		//printSet.writeAsCsv(Config.outputPath()+"/" + SampleTest.class.getName(), WriteMode.OVERWRITE);
		//OperatorTree tree = new OperatorTree(env );
       // env.execute();
		//TupleGenerator tg = new TupleGenerator(tree.createOperatorTree(), env,2);
		TupleGenerator tg = new TupleGenerator(env,3);
	}

	@FunctionAnnotation.ForwardedFields("f0.f0->f0;f0.f1->f1;f1.f1->f2")
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

	public static class RankFilter
			implements
			FilterFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> {

		// Returns true if PageRank is greater than 2
		public boolean filter(
				Tuple2<Tuple2<String, String>, Tuple2<String, Long>> joinSet)
				throws Exception {

			return joinSet.f1.f1 > 3;
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

    public static class VisitsHadoopReader implements
            FlatMapFunction<Tuple2<LongWritable,Text>, Tuple2<String, String>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        // Reads Visit data-set from flat file into tuples of <User,URL>
        public void flatMap(Tuple2<LongWritable, Text> readLineFromFile, Collector<Tuple2<String, String>> collector) throws Exception {
            String line = readLineFromFile.f1.toString();
            if (!line.startsWith("%")) {
                String[] tokens = SEPARATOR.split(line);

                String user = tokens[0];
                String url = tokens[1];

                collector.collect(new Tuple2<String, String>(user, url));
            }
        }
    }

	public static class VisitsPOJAReader implements
			FlatMapFunction<String, Visits> {

		private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		// Reads Visit data-set from flat file into tuples of <User,URL>
		public void flatMap(String readLineFromFile,
				Collector<Visits> collector) throws Exception {

			if (!readLineFromFile.startsWith("%")) {
				String[] tokens = SEPARATOR.split(readLineFromFile);

				String user = tokens[2];
				String url = tokens[1];

				collector.collect(new Visits(url, user));
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

    public static class UnionReader implements
            FlatMapFunction<String, Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> {

        private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        public void flatMap(String readLineFromFile, Collector<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> out) throws Exception {
            if (!readLineFromFile.startsWith("%")) {
                String[] tokens = SEPARATOR.split(readLineFromFile);

                String user = tokens[0];
                String url = tokens[1];
                Long pageRank = Long.parseLong(tokens[3]);
                Tuple2<String,String> one = new Tuple2<String, String>(user,url);
                Tuple2<String,Long> two =  new Tuple2<String, Long>(url,pageRank);
                out.collect(new Tuple2<Tuple2<String, String>, Tuple2<String, Long>> (one,two));
            }
        }
    }

	public static class ResultGrouper implements
			ReduceFunction<Tuple3<String, String, Long>> {

		public Tuple3<String, String, Long> reduce(
				Tuple3<String, String, Long> arg0,
				Tuple3<String, String, Long> arg1) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple3<String, String, Long>(arg0.f0, arg1.f0, arg0.f2
					+ arg1.f2);
		}

	}

	public static class RankGrouper implements
			ReduceFunction<Tuple2<String, Long>> {

		public Tuple2<String, Long> reduce(Tuple2<String, Long> arg0,
				Tuple2<String, Long> arg1) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, Long>(arg0.f0, arg0.f1 + arg1.f1);
		}

	}
}

