package flink.examplegeneration.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

/**
 * A Class that defines/sets an operator's equivalence classes, who has only single equivalence class
 * having one or more example
 */
public class SingleEquivalenceClass {
	
	private List<EquivalenceClass> singleEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass singleExample;
	
	public EquivalenceClass getSingleExample() {
		return singleExample;
	}

	public void setSingleExample(EquivalenceClass singleExample) {
		this.singleExample = singleExample;
	}

    /**
     * Creates new instance of operator's equivalence class, with just an example being the
     * only factor under consideration
     */
    public SingleEquivalenceClass() {
		this.singleExample = new EquivalenceClass("SingleOperatorExample");
		this.singleEquivalenceClasses.add(singleExample);
	}

}
