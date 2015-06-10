package thesis.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

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

	public UnionCrossEquivalenceClasses() {
		this.firstTableExample = new EquivalenceClass("FirstTableExample");
		this.secondTableExample = new EquivalenceClass("SecondTableExample");
		this.unionEquivalenceClasses.add(firstTableExample);
		this.unionEquivalenceClasses.add(secondTableExample);
	}

}
