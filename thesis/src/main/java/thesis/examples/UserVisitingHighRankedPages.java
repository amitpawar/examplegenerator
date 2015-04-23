package thesis.examples;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

public class UserVisitingHighRankedPages {

	public static void main(String args[]) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> visits = env.readTextFile(Config.pathToVisits());

		DataSource<String> urls = env.readTextFile(Config.pathToUrls());

		// Reads visit data-set in the Tuples of <User,URL>
		DataSet<Tuple2<String, String>> visitSet = visits.flatMap(
				new VisitsReader()).distinct();

		// Reads url data-set in the Tuples of <URL,PageRank>
		DataSet<Tuple2<String, Long>> urlSet = urls.flatMap(new URLsReader())
				.distinct();

		// Joins the visit and url data-set on URL
		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> joinSet = visitSet
				.join(urlSet).where(1).equalTo(0);

		// Filters the joined set with PageRank > 3
		DataSet<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>> finalSet = joinSet
				.filter(new RankFilter());

		// Merges the tuples to create tuples of <User,URL,PageRank>
		DataSet<Tuple3<String, String, Long>> printSet = finalSet
				.flatMap(new PrintResult());

		
		printSet.writeAsCsv(Config.outputPath()+"/"+UserVisitingHighRankedPages.class.getName(),WriteMode.OVERWRITE);

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

}