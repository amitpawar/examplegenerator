package flink.examplegeneration.algorithm.semantics;

import java.util.List;

public class EquivalenceClass {
	
	private String name;
	private boolean hasExample = false;
    private List examples;


    public List getExamples() {
        return examples;
    }

    public void setExamples(List examples) {
        this.examples = examples;
    }
	
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
