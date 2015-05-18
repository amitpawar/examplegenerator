package thesis.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

public class LoadEquivalenceClasses {
	
	private List<EquivalenceClass> loadEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass loadExample;
	
	public EquivalenceClass getLoadExample() {
		return loadExample;
	}

	public void setLoadExample(EquivalenceClass loadExample) {
		this.loadExample = loadExample;
	}

	public LoadEquivalenceClasses() {
		this.loadExample = new EquivalenceClass("LoadExample");
		this.loadEquivalenceClasses.add(loadExample);
	}

}
