package thesis.algorithm.semantics;

import java.util.List;

public class LoadEquivalenceClasses {
	
	private List<EquivalenceClass> loadEquivalenceClasses;
	EquivalenceClass baseTableExample;
	
	public EquivalenceClass getBaseTableExample() {
		return baseTableExample;
	}

	public void setBaseTableExample(EquivalenceClass baseTableExample) {
		this.baseTableExample = baseTableExample;
	}

	public LoadEquivalenceClasses() {
		this.baseTableExample = new EquivalenceClass("BaseTableExample");
		this.loadEquivalenceClasses.add(baseTableExample);
	}

}
