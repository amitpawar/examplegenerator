package thesis.examples;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;



public class UserPageVisitsGroupedLog {

	public static void main(String args[]) throws Exception{
		

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> visits = env.readTextFile(Config.pathToVisits());

		// Reads visit data-set in the Tuples of <User,URL>
		DataSet<Tuple3<String, String, Integer>> visitSet = visits.flatMap(
				new VisitsReaderWithCount());
		
		DataSet<Tuple3<String,String,Integer>> groupedFilteredSet = visitSet.groupBy(1).reduce(new GroupCounter()).filter(new HighVisitsFilter()); 
		
		groupedFilteredSet.writeAsCsv(Config.outputPath()+"/"+UserPageVisitsGroupedLog.class.getName(),WriteMode.OVERWRITE);

		env.execute();
	}

	public static class VisitsReaderWithCount implements
			FlatMapFunction<String, Tuple3<String, String, Integer>> {

		private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		// Reads Visit data-set from flat file into tuples of <User,URL>
		public void flatMap(String readLineFromFile,
				Collector<Tuple3<String, String, Integer>> collector) throws Exception {

			if (!readLineFromFile.startsWith("%")) {
				String[] tokens = SEPARATOR.split(readLineFromFile);

				String user = tokens[0];
				String url = tokens[1];

				collector.collect(new Tuple3<String, String, Integer>(user, url, 1));
			}
		}
	}
	
	public static class GroupCounter implements ReduceFunction<Tuple3<String,String,Integer>>{

		public Tuple3<String, String, Integer> reduce(
				Tuple3<String, String, Integer> arg0,
				Tuple3<String, String, Integer> arg1) throws Exception {
			return new Tuple3<String, String, Integer>(arg0.f0,arg0.f1,arg0.f2+arg1.f2);
		}
		
	}
	
	public static class HighVisitsFilter implements FilterFunction<Tuple3<String,String,Integer>>{

		public boolean filter(Tuple3<String, String, Integer> groupedSet)
				throws Exception {
			return groupedSet.f2 > 3;
		}
		
	}
}
