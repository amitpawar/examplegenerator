package thesis.algorithm.semantics;

import java.util.List;


public class FilterEquivalenceClasses {
	
	private List<EquivalenceClass> filterEquivalenceClasses;
	EquivalenceClass filterPass;
	EquivalenceClass filterFail;
	
	public FilterEquivalenceClasses() {
		
		this.filterPass = new EquivalenceClass("FilterPass");
		this.filterFail = new EquivalenceClass("FilterFail");
		this.filterEquivalenceClasses.add(filterPass);
		this.filterEquivalenceClasses.add(filterFail);
		
	}
	

}
