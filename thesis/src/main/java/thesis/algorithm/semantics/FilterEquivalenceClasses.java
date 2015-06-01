package thesis.algorithm.semantics;

import java.util.List;


public class FilterEquivalenceClasses{
	
	private List<EquivalenceClass> filterEquivalenceClasses;
	EquivalenceClass filterPass;
	EquivalenceClass filterFail;
	
	public EquivalenceClass getFilterPass() {
		return filterPass;
	}

	public void setFilterPass(EquivalenceClass filterPass) {
		this.filterPass = filterPass;
	}

	public EquivalenceClass getFilterFail() {
		return filterFail;
	}

	public void setFilterFail(EquivalenceClass filterFail) {
		this.filterFail = filterFail;
	}

	public FilterEquivalenceClasses() {
		this.filterPass = new EquivalenceClass("FilterPass");
		this.filterFail = new EquivalenceClass("FilterFail");
		this.filterEquivalenceClasses.add(filterPass);
		this.filterEquivalenceClasses.add(filterFail);
		
	}

}
