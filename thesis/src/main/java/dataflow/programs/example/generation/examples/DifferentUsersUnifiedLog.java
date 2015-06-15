package dataflow.programs.example.generation.examples;

import dataflow.programs.example.generation.input.operatortree.OperatorTree;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.api.common.operators.TupleGenerator;
import dataflow.programs.example.generation.examples.UserPageVisitsGroupedLog.VisitsReaderWithCount;

public class DifferentUsersUnifiedLog {

	public static void main(String[] args) throws Exception {
		

		ExecutionEnvironment env = ExecutionEnvironment
				.createCollectionsEnvironment();

		DataSource<String> visits = env.readTextFile(Config.pathToVisits());
		DataSource<String> visitsEU = env.readTextFile(Config.pathToVisitsEU());
		
		DataSet<Tuple3<String, String, Integer>> visitSet = visits.flatMap(
				new VisitsReaderWithCount());

		
		DataSet<Tuple3<String, String, Integer>> visitEUSet = visitsEU.flatMap(new VisitsReaderWithCount());

		
		DataSet<Tuple3<String, String, Integer>> visitsUnion = visitSet.union(visitEUSet);
		
		//DataSet<Tuple3<String,String,Integer>> groupedFilteredSet = visitsUnion.groupBy(1).reduce(new GroupCounter()).filter(new HighVisitsFilter());
		
		//groupedFilteredSet.writeAsCsv(Config.outputPath()+"/"+DifferentUsersUnifiedLog.class.getName(),WriteMode.OVERWRITE);

		visitsUnion.print();
		OperatorTree tree = new OperatorTree(env);
		//tree.createOperatorTree();
		TupleGenerator tg = new TupleGenerator(tree.createOperatorTree(), env,2);
		//env.execute();
		//OperatorTree tree = new OperatorTree(env);
		//tree.createOperatorTree();
	}

}
