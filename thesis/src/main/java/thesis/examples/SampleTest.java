package thesis.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.operators.base.FlatMapOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.operators.translation.PlanFilterOperator;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.api.java.operators.translation.PlanFilterOperator.FlatMapFilter;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.client.program.PackagedProgram.PreviewPlanEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plandump.DumpableConnection;
import org.apache.flink.optimizer.plandump.DumpableNode;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.mahout.classifier.df.data.Data;

import thesis.algorithm.logic.TupleGenerator;
import thesis.input.datasources.InputDataSource;
import thesis.input.operatortree.OperatorTree;

public class SampleTest {

	

	public static void main(String[] args) throws Exception {

		List<InputDataSource> dataSources = new ArrayList<InputDataSource>();
		List<DataSet<?>> dataSets = new ArrayList<DataSet<?>>();
		
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> visits = env.readTextFile(Config.pathToVisits());
		DataSource<String> urls = env.readTextFile(Config.pathToUrls());
		
		
		
		DataSet<Tuple2<String, String>> visitSet = visits.flatMap(
				new VisitsReader()).distinct();
		dataSets.add(visitSet);
		InputDataSource input1 = new InputDataSource();
		input1.setDataSet(visitSet);
		input1.setName("Visits");
		input1.setId(0);
		
		//DataSet<Visits> visitSet = visits.flatMap(new VisitsPOJAReader());

		DataSet<Tuple2<String, Long>> urlSet = urls.flatMap(new URLsReader())
				.distinct();
		dataSets.add(urlSet);
		InputDataSource input2 = new InputDataSource();
		input2.setDataSet(urlSet);
		input2.setName("Urls");
		input2.setId(1);
		
		dataSources.add(input1);
		dataSources.add(input2);

		/*DataSet<Tuple2<Visits, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);*/
		
		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);
		
		//DataSet<Tuple2<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple2<Tuple2<String, String>, Tuple2<String, Long>>>> crossSet = joinSet.cross(joinSet);

		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> filterSet = joinSet
				.filter(new RankFilter());

		DataSet<Tuple3<String, String, Long>> printSet = filterSet.project(1);
		// .flatMap(new PrintResult());

		//crossSet.print();
		printSet.print();
		
		OperatorTree tree = new OperatorTree(env, dataSources );
		//tree.createOperatorTree();
		TupleGenerator tg = new TupleGenerator(dataSources, tree.createOperatorTree(), env);
		//tg.generateTuplesTest(env, dataSets, tree.createOperatorTree());
		//printSet.writeAsCsv(Config.outputPath()+"/" + SampleTest.class.getName(), WriteMode.OVERWRITE);
		//env.execute();
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

	public static class RankFilter
			implements
			FilterFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> {

		// Returns true if PageRank is greater than 2
		public boolean filter(
				Tuple2<Tuple2<String, String>, Tuple2<String, Long>> joinSet)
				throws Exception {

			return joinSet.f1.f1 > 2;
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


