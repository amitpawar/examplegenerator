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

import thesis.algorithm.logic.TupleGenerator;
import thesis.input.operatortree.OperatorTree;

public class SampleTest {

	

	public static void main(String[] args) throws Exception {

		List<DataSet<?>> dataSources = new ArrayList<DataSet<?>>();
		
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> visits = env.readTextFile(Config.pathToVisits());
		DataSource<String> urls = env.readTextFile(Config.pathToUrls());
		
		
		DataSet<Tuple2<String, String>> visitSet = visits.flatMap(
				new VisitsReader()).distinct();
		//DataSet<Visits> visitSet = visits.flatMap(new VisitsPOJAReader());

		DataSet<Tuple2<String, Long>> urlSet = urls.flatMap(new URLsReader())
				.distinct();
		
		dataSources.add(visitSet);
		dataSources.add(urlSet);

		/*DataSet<Tuple2<Visits, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);*/
		
		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);

		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> filterSet = joinSet
				.filter(new RankFilter());

		DataSet<Tuple3<String, String, Long>> printSet = filterSet.project(1);
		// .flatMap(new PrintResult());

		/*
		 * printSet.writeAsCsv(Config.outputPath()+"/" +
		 * SampleTest.class.getName(), WriteMode.OVERWRITE);
		 */
		printSet.print();

		/*
		 * JavaPlan plan = env.createProgramPlan(); Optimizer compiler = new
		 * Optimizer(); compiler.setDefaultParallelism(1); OptimizedPlan opPlan
		 * = compiler.compile(plan);
		 * 
		 * for(PlanNode node : opPlan.getAllNodes()){ Operator<?> operator =
		 * node.getOptimizerNode().getOperator(); operator.setParallelism(1);
		 * System
		 * .out.println("Node "+node.getOptimizerNode().getOperator().getName
		 * ());
		 * 
		 * if(operator instanceof JoinOperatorBase){ if(operator instanceof
		 * DualInputOperator) {
		 * System.out.println("Trueeeeeeeeeeeeeeeeeeeee thattttttttt");
		 * System.out.println(((JoinOperatorBase)
		 * operator).getNumberOfInputs()); }
		 * 
		 * //System.out.println(((FilterOperatorBase)
		 * operator).getOperatorInfo().getInputType());
		 * //System.out.println(((FilterOperatorBase)
		 * operator).getOperatorInfo().getOutputType());
		 * 
		 * } if(operator instanceof PlanProjectOperator){
		 * System.out.println("Falseeeeeeeeeeeeeeeeeeeeeee");
		 * System.out.println(((PlanProjectOperator)
		 * operator).getOperatorInfo().getInputType());
		 * System.out.println(((PlanProjectOperator)
		 * operator).getOperatorInfo().getOutputType());
		 * 
		 * }
		 * 
		 * if(operator instanceof FilterOperatorBase){
		 * System.out.println("FILTERRRRRRRRRRRRRRRR");
		 * System.out.println(((FilterOperatorBase)
		 * operator).getNumberOfInputs());
		 * System.out.println(((FilterOperatorBase)
		 * operator).getOperatorInfo().getInputType());
		 * System.out.println(((FilterOperatorBase)
		 * operator).getOperatorInfo().getOutputType());
		 * System.out.println(((FilterOperatorBase
		 * )operator).getUserCodeWrapper().getUserCodeObject()); Object test =
		 * ((
		 * FilterOperatorBase)operator).getUserCodeWrapper().getUserCodeObject(
		 * );
		 * 
		 * if(test instanceof FlatMapFilter){
		 * System.out.println("Yahoooooooooooooooooooooooooooooooo");
		 * System.out.println(((FlatMapFilter)test).getWrappedFunction()); } }
		 * 
		 * if(node.getOptimizerNode().getOutgoingConnections() != null) {
		 * for(DagConnection conn :
		 * node.getOptimizerNode().getOutgoingConnections()){
		 * System.out.println(
		 * "OUTGOING-----"+conn.getTarget().getOperator().getName()); } }
		 * for(DumpableConnection<OptimizerNode> input :
		 * node.getOptimizerNode().getDumpableInputs() ){ if(input!=null)
		 * System.out.println("Input "+input); }
		 * 
		 * System.out.println("Output "+node.getOptimizerNode().getOperator().
		 * getOperatorInfo().getOutputType().toString());
		 * System.out.println("Config "
		 * +node.getOptimizerNode().getOperator().getParameters());
		 * //System.out.println(node.getNodeName());
		 * 
		 * for(Channel channel : node.getInputs()){
		 * System.out.println("Source: "+channel.getSource().getNodeName());
		 * System.out.println(": ");
		 * System.out.println("Target: "+channel.getTarget().getNodeName());
		 * 
		 * } }
		 * 
		 * PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
		 * System.out.println(dumper.getOptimizerPlanAsJSON(opPlan));
		 * PreviewPlanEnvironment pEnv = new PreviewPlanEnvironment();
		 * pEnv.setAsContext(); List<DataSinkNode> previwPlan =
		 * Optimizer.createPreOptimizedPlan(plan);
		 * System.out.println(dumper.getPactPlanAsJSON(previwPlan));
		 * System.out.println("PactPlan-----------------"); for(DataSinkNode
		 * sinkNode : previwPlan){ System.out.println("Input"
		 * +sinkNode.getInputConnection().toString());
		 * System.out.println("Output "
		 * +sinkNode.getOutgoingConnections().toString());
		 * 
		 * }
		 * 
		 * for(SinkPlanNode sourceNode : opPlan.getDataSinks()){
		 * System.out.println("Source---------"+sourceNode.getNodeName()); }
		 */

		OperatorTree tree = new OperatorTree(env);
		//tree.createOperatorTree();
		TupleGenerator tg = new TupleGenerator();
		tg.generateTuples(dataSources, tree.createOperatorTree());
		 env.execute();
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


