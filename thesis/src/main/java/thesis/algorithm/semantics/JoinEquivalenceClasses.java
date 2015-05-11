package thesis.algorithm.semantics;

import java.util.List;

public class JoinEquivalenceClasses {
	
	private List<EquivalenceClass> joinEquivalenceClasses;
	EquivalenceClass joinedExample;
	
	public JoinEquivalenceClasses() {
		this.joinedExample = new EquivalenceClass("JoinedExample");
		this.joinEquivalenceClasses.add(joinedExample);
	}

}
