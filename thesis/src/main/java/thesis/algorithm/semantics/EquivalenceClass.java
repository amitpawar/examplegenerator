package thesis.algorithm.semantics;

public class EquivalenceClass {
	
	private String name;
	private int dataSetCount;
	private boolean hasExample;
	
	public EquivalenceClass(String className) {
		this.name = className;
	}
	
	public boolean hasExample() {
		return hasExample;
	}
	public void setHasExample(boolean hasExample) {
		this.hasExample = hasExample;
	}
	public String getName() {
		return name;
	}
	
	public int getDataSetCount() {
		return dataSetCount;
	}
	public void setDataSetCount(int dataSetCount) {
		this.dataSetCount = dataSetCount;
	}
	

}
