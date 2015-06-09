package thesis.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import org.apache.flink.api.common.operators.TupleGenerator;
import thesis.examples.UserPageVisitsGroupedLog.GroupCounter;
import thesis.examples.UserPageVisitsGroupedLog.HighVisitsFilter;
import thesis.examples.UserPageVisitsGroupedLog.VisitsReaderWithCount;
import thesis.input.operatortree.OperatorTree;
import thesis.input.datasources.*;

public class DifferentUsersUnifiedLog {

	public static void main(String[] args) throws Exception {
		
		List<InputDataSource> inputSources = new ArrayList<InputDataSource>();
	
		ExecutionEnvironment env = ExecutionEnvironment
				.createCollectionsEnvironment();

		DataSource<String> visits = env.readTextFile(Config.pathToVisits());
		DataSource<String> visitsEU = env.readTextFile(Config.pathToVisitsEU());
		
		DataSet<Tuple3<String, String, Integer>> visitSet = visits.flatMap(
				new VisitsReaderWithCount());
		InputDataSource source1 = new InputDataSource();
		source1.setDataSet(visitSet);
		source1.setId(0);
		source1.setName("Visit");
		
		DataSet<Tuple3<String, String, Integer>> visitEUSet = visitsEU.flatMap(new VisitsReaderWithCount());
		InputDataSource source2 = new InputDataSource();
		source2.setDataSet(visitEUSet);
		source2.setId(1);
		source2.setName("VisitEU");
		
		inputSources.add(source1);
		inputSources.add(source2);
		
		DataSet<Tuple3<String, String, Integer>> visitsUnion = visitSet.union(visitEUSet);
		
		DataSet<Tuple3<String,String,Integer>> groupedFilteredSet = visitsUnion.groupBy(1).reduce(new GroupCounter()).filter(new HighVisitsFilter()); 
		
		groupedFilteredSet.writeAsCsv(Config.outputPath()+"/"+DifferentUsersUnifiedLog.class.getName(),WriteMode.OVERWRITE);

		OperatorTree tree = new OperatorTree(env);
		//tree.createOperatorTree();
		TupleGenerator tg = new TupleGenerator(inputSources, tree.createOperatorTree(), env,2);
		//env.execute();
		//OperatorTree tree = new OperatorTree(env);
		//tree.createOperatorTree();
	}

}
