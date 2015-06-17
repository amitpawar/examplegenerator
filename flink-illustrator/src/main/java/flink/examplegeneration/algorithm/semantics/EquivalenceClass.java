package flink.examplegeneration.algorithm.semantics;

import java.util.List;

/**
 * The equivalence class object, each operator may have one or more equivalence class
 */
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

    /**
     * Creates new instance of equivalence class for a given name
     * @param className
     */
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
