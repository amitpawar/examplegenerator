package thesis.algorithm.semantics;

public class EquivalenceClass {
	
	private String name;
	private boolean hasExample = false;
	
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


}
