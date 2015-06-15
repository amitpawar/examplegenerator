package dataflow.programs.example.generation.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

public class JoinEquivalenceClasses {
	
	private List<EquivalenceClass> joinEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass joinedExample;
	
	public EquivalenceClass getJoinedExample() {
		return joinedExample;
	}

	public void setJoinedExample(EquivalenceClass joinedExample) {
		this.joinedExample = joinedExample;
	}

	public JoinEquivalenceClasses() {
		this.joinedExample = new EquivalenceClass("JoinedExample");
		this.joinEquivalenceClasses.add(joinedExample);
	}

}
