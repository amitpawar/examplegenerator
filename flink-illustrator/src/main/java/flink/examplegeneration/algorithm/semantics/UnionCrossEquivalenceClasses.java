package flink.examplegeneration.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

/**
 * A Class that defines/sets unions or cross operator's equivalence classes
 */
public class UnionCrossEquivalenceClasses {
	
	private List<EquivalenceClass> unionEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass firstTableExample;
	EquivalenceClass secondTableExample;

	public EquivalenceClass getFirstTableExample() {
		return firstTableExample;
	}

	public void setFirstTableExample(EquivalenceClass firstTableExample) {
		this.firstTableExample = firstTableExample;
	}

	public EquivalenceClass getSecondTableExample() {
		return secondTableExample;
	}

	public void setSecondTableExample(EquivalenceClass secondTableExample) {
		this.secondTableExample = secondTableExample;
	}

    /**
     * Creates new instance of union or cross operator's equivalence class, one from first input
     * other from second input
     */
    public UnionCrossEquivalenceClasses() {
		this.firstTableExample = new EquivalenceClass("FirstTableExample");
		this.secondTableExample = new EquivalenceClass("SecondTableExample");
		this.unionEquivalenceClasses.add(firstTableExample);
		this.unionEquivalenceClasses.add(secondTableExample);
	}

}
