package thesis.algorithm.semantics;

import java.util.List;

public class LoadEquivalenceClasses {
	
	private List<EquivalenceClass> loadEquivalenceClasses;
	EquivalenceClass baseTableExample;
	
	public LoadEquivalenceClasses() {
		this.baseTableExample = new EquivalenceClass("BaseTableExample");
		this.loadEquivalenceClasses.add(baseTableExample);
	}

}
