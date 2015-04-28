package thesis.input.datasources;

import org.apache.flink.api.java.DataSet;

public class InputDataSource {
	
	private DataSet<?> dataSet;
	private int id;
	private String name;
	
	public DataSet<?> getDataSet() {
		return dataSet;
	}
	public void setDataSet(DataSet<?> dataSet) {
		this.dataSet = dataSet;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	

}
