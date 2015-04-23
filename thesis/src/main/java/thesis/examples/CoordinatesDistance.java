package thesis.examples;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class CoordinatesDistance {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> coordSet1 = env.readTextFile(Config
				.pathToCoordSet1());
		DataSource<String> coordSet2 = env.readTextFile(Config
				.pathToCoordSet2());

		DataSet<Coord> set1 = coordSet1.flatMap(new CoordsReader());
		DataSet<Coord> set2 = coordSet2.flatMap(new CoordsReader());
		
		DataSet<Tuple3<Integer, Integer, Double>> distances =
				set1.cross(set2).with(new EuclideanDistComputer());
		
		distances.print();
		
		env.execute();

	}

	public static class CoordsReader implements FlatMapFunction<String, Coord> {

		private final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		public void flatMap(String readLineFromFile, Collector<Coord> out)
				throws Exception {

			if (!readLineFromFile.startsWith("%")) {
				String[] tokens = SEPARATOR.split(readLineFromFile);

				int id = Integer.parseInt(tokens[0]);
				int x = Integer.parseInt(tokens[1]);
				int y = Integer.parseInt(tokens[2]);

				out.collect(new Coord(id, x, y));
			}

		}

	}

	public static class EuclideanDistComputer implements
			CrossFunction<Coord, Coord, Tuple3<Integer, Integer, Double>> {

		public Tuple3<Integer, Integer, Double> cross(Coord val1, Coord val2)
				throws Exception {
			 
			double dist = Math.sqrt(Math.pow(val1.x - val2.x, 2) + Math.pow(val1.y - val2.y, 2));
			return new Tuple3<Integer, Integer, Double>(val1.id, val2.id, dist);
		}

		
	}

}
