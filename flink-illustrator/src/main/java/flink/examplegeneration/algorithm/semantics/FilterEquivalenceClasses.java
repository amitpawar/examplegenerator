package flink.examplegeneration.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

/**
 * A Class that defines/sets filter operator's equivalence classes
 */
public class FilterEquivalenceClasses{
	
	private List<EquivalenceClass> filterEquivalenceClasses = new ArrayList<EquivalenceClass>();
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

    /**
     * Creates new instance of filter operator's equivalence class, one that passes filter
     * other that fails the filtering predicate
     */
	public FilterEquivalenceClasses() {
		this.filterPass = new EquivalenceClass("FilterPass");
		this.filterFail = new EquivalenceClass("FilterFail");
		this.filterEquivalenceClasses.add(filterPass);
		this.filterEquivalenceClasses.add(filterFail);
		
	}

}
