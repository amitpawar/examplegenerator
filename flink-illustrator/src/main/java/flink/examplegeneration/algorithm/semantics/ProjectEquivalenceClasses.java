package flink.examplegeneration.algorithm.semantics;

import java.util.ArrayList;
import java.util.List;

public class ProjectEquivalenceClasses {
	
	private List<EquivalenceClass> projectEquivalenceClasses = new ArrayList<EquivalenceClass>();
	EquivalenceClass projectExample;
	
	public EquivalenceClass getProjectExample() {
		return projectExample;
	}

	public void setProjectExample(EquivalenceClass projectExample) {
		this.projectExample = projectExample;
	}

	public ProjectEquivalenceClasses() {
		this.projectExample = new EquivalenceClass("ProjectExample");
		this.projectEquivalenceClasses.add(projectExample);
	}
}
