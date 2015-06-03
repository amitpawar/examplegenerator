package thesis.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

public class SingleEquivalenceClass {
	
	private List<EquivalenceClass> singleEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass singleExample;
	
	public EquivalenceClass getSingleExample() {
		return singleExample;
	}

	public void setSingleExample(EquivalenceClass singleExample) {
		this.singleExample = singleExample;
	}

	public SingleEquivalenceClass() {
		this.singleExample = new EquivalenceClass("LoadExample");
		this.singleEquivalenceClasses.add(singleExample);
	}

}
