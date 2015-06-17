package flink.examplegeneration.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

/**
 * A Class that defines/sets join operator's equivalence classes
 */
public class JoinEquivalenceClasses {
	
	private List<EquivalenceClass> joinEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass joinedExample;
	
	public EquivalenceClass getJoinedExample() {
		return joinedExample;
	}

	public void setJoinedExample(EquivalenceClass joinedExample) {
		this.joinedExample = joinedExample;
	}

    /**
     * Creates new instance of join operator's equivalence class, with joined example being the
     * only factor under consideration
     */
    public JoinEquivalenceClasses() {
		this.joinedExample = new EquivalenceClass("JoinedExample");
		this.joinEquivalenceClasses.add(joinedExample);
	}

}
