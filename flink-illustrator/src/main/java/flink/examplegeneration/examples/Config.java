package flink.examplegeneration.examples;

public class Config {


	public static String pathToVisits1() {
		return "src/resources/visits";
	}

	public static String pathToVisitsEU() {
		return "src/resources/visitseu";
	}

	public static String pathToVisits(){return "hdfs://localhost:9000/thesis/visits";}



	public static String pathToUrls() {
		return "src/resources/urls";
	}

	public static String pathToCoordSet1() {
		return "/home/amit/thesis/dataflow/coordSet1";
	}

	public static String pathToCoordSet2() {
		return "/home/amit/thesis/dataflow/coordSet2";
	}

	public static String pathToSet1(){return "src/resources/CrossInput1";}

    public static String pathToSet2(){return "src/resources/CrossInput2";}


	public static String outputPath() {
		return "src/resources/output";
	}
}