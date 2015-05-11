package thesis.algorithm.semantics;

import java.util.List;

public class UnionCrossEquivalenceClasses {
	
	private List<EquivalenceClass> unionEquivalenceClasses;
	EquivalenceClass firstTableExample;
	EquivalenceClass secondTableExample;
	
	public UnionCrossEquivalenceClasses() {
		this.firstTableExample = new EquivalenceClass("FirstTableExample");
		this.secondTableExample = new EquivalenceClass("SecondTableExample");
		this.unionEquivalenceClasses.add(firstTableExample);
		this.unionEquivalenceClasses.add(secondTableExample);
	}

}
