package thesis.input.operatortree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.operators.base.FlatMapOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.operators.translation.PlanFilterOperator;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import thesis.input.datasources.InputDataSource;
import thesis.input.operatortree.SingleOperator.JUCCondition;

public class OperatorTree {

	private JavaPlan javaPlan;
	private Optimizer optimizer;
	private OptimizedPlan optimizedPlan;
	private List<SingleOperator> operatorTree;
	private List<String> addedNodes;
	private List<InputDataSource> dataSets;
	private int dataSetId = 0;
	private boolean isDualInputOperator = false;
	
	private enum InputNum { 
		FIRST(0),SECOND(1);
		private int value;
		private InputNum(int val){
			this.value = val;
		}
		private int getValue(){
			return this.value;
		}
	};

	public OperatorTree(ExecutionEnvironment env, List<InputDataSource> dataSets) {
		this.javaPlan = env.createProgramPlan();
		this.optimizer = new Optimizer(new DataStatistics(),new DefaultCostEstimator(), new Configuration());
		this.optimizedPlan = this.optimizer.compile(this.javaPlan);
		this.operatorTree = new ArrayList<SingleOperator>();
		this.addedNodes = new ArrayList<String>();
		this.dataSets = dataSets;
		
	}
	
	private boolean isVisited(Operator<?> operator) {
		return (this.addedNodes.contains(operator.getName()));
	}

	public List<SingleOperator> createOperatorTree() {
		
		for (SourcePlanNode sourceNode : this.optimizedPlan.getDataSources()) {
			
			List<Integer> inputDataSet = new ArrayList<Integer>();
			inputDataSet.add(this.dataSetId);
			
			if (!isVisited(sourceNode.getProgramOperator())) {
				SingleOperator op = new SingleOperator();
				op.setOperatorType(OperatorType.SOURCE);
				op.setOperatorInputDataSetId(inputDataSet);
				op.setOperator(sourceNode.getProgramOperator());
				op.setOperatorName(sourceNode.getNodeName());
				op.setOperatorOutputType(sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType());
				this.operatorTree.add(op);
				this.addedNodes.add(sourceNode.getNodeName());
				if (sourceNode.getOptimizerNode().getOutgoingConnections() != null)
					addOutgoingNodes(sourceNode.getOptimizerNode().getOutgoingConnections(), inputDataSet);
				this.dataSetId++;
			}
		}
		displayItems();
		
		return this.operatorTree;
	}

	public void addOutgoingNodes(List<DagConnection> outgoingConnections, List<Integer> inputDatasets) {
		
		for (DagConnection conn : outgoingConnections) {
			SingleOperator op = new SingleOperator();
			OptimizerNode node = conn.getTarget().getOptimizerNode();
			this.isDualInputOperator = (node.getOperator() instanceof DualInputOperator)? true:false;
			
			if(this.isDualInputOperator && this.dataSetId % 2 == 0)
				return;
			else
				addNode(node, inputDatasets);
			if (node.getOutgoingConnections() != null)
				addOutgoingNodes(node.getOutgoingConnections(), inputDatasets);
		}
	}

	@SuppressWarnings("rawtypes")
	public void addNode(OptimizerNode node, List<Integer> inputDatasets) {
		
		Operator<?> operator = node.getOperator();
		SingleOperator opToAdd = new SingleOperator();
	
		
		if(operator instanceof FlatMapOperatorBase){
			//System.out.println("Testststs"+((FlatMapOperatorBase) operator).getInput().getClass());
			if(((FlatMapOperatorBase) operator).getInput() instanceof GenericDataSourceBase){
			
				if(!isVisited(operator)){
					
					opToAdd.setOperatorType(OperatorType.LOAD);
					addOperatorDetails(opToAdd, operator, inputDatasets);
				}
			}
		}
		
		if (operator instanceof JoinOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.JOIN);
				SingleOperator opToAddWithJoinPred = addJoinOperatorDetails((JoinOperatorBase) operator, opToAdd);
				addOperatorDetails(opToAddWithJoinPred, operator, inputDatasets);
				this.dataSetId++;
			}
		}

		if (operator instanceof CrossOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.CROSS);
				addOperatorDetails(opToAdd, operator, inputDatasets);
				this.dataSetId++;
			}
		}

		if (operator instanceof FilterOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.FILTER);
				addOperatorDetails(opToAdd, operator, inputDatasets);
			}
		}
		
		if (operator instanceof Union<?>) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.UNION);
				addOperatorDetails(opToAdd, operator, inputDatasets);
				this.dataSetId++;
			}
		}
		
		if (operator instanceof PlanProjectOperator) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.PROJECT);
				
				opToAdd.setProjectColumns(getProjectArray(operator.getName()));
				addOperatorDetails(opToAdd, operator, inputDatasets);
			}
		}

		if (operator instanceof GroupReduceOperatorBase) {
			if (operator.getName().contains("Distinct")) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.DISTINCT);
					addOperatorDetails(opToAdd, operator, inputDatasets);
				}
			}
		}
		
	}
	
	public void addOperatorDetails(SingleOperator opToAdd, Operator<?> operator, List<Integer> inputDatasets){
		opToAdd.setOperatorName(operator.getName());
		opToAdd.setOperator(operator);
		opToAdd.setOperatorInputType(addInputTypes(operator));
		opToAdd.setOperatorInputDataSetId(inputDatasets);
		opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
		this.operatorTree.add(opToAdd);
		this.addedNodes.add(operator.getName());
	}
	
	@SuppressWarnings("rawtypes")
	public List<TypeInformation<?>> addInputTypes(Operator<?> operator){
		
		List<TypeInformation<?>> inputTypes = new ArrayList<TypeInformation<?>>();
		
		if(operator instanceof DualInputOperator){
			inputTypes.add(((DualInputOperator)operator).getOperatorInfo().getFirstInputType());
			inputTypes.add(((DualInputOperator)operator).getOperatorInfo().getSecondInputType());
		}
		
		if(operator instanceof SingleInputOperator){
			inputTypes.add(((SingleInputOperator)operator).getOperatorInfo().getInputType());
		}
		
		return inputTypes;
	}
	
	@SuppressWarnings("rawtypes")
	public SingleOperator addJoinOperatorDetails(JoinOperatorBase joinOperator, SingleOperator opToAdd){
			
		int[] firstInputKeys = joinOperator.getKeyColumns(InputNum.FIRST.getValue());
		int[] secondInputKeys = joinOperator.getKeyColumns(InputNum.SECOND.getValue());
		
		JUCCondition joinPred = opToAdd.new JUCCondition();
		
		joinPred.setFirstInput(InputNum.FIRST.getValue());
		joinPred.setSecontInput(InputNum.SECOND.getValue());
		joinPred.setFirstInputKeyColumns(firstInputKeys);
		joinPred.setSecondInputKeyColumns(secondInputKeys);
		
		opToAdd.setJUCCondition(joinPred);
		
		return opToAdd;
	
	}
	
	public int[] getProjectArray(String operatorName){
	
		Pattern separator = Pattern.compile("[\\[,\\]]");
		String[] tokens = separator.split(operatorName);
		
		int[] projectArray = new int[tokens.length-1];
		for(int ctr = 0; ctr < tokens.length-1; ctr++){
			projectArray[ctr] = Integer.parseInt(tokens[ctr+1].replaceAll("\\s", ""));
		}
		return projectArray;
	}
	
	public void displayItems(){
		
		for (int i = 0; i < this.operatorTree.size(); i++) {
		/*	if(!(this.operatorTree.get(i).getOperatorInputType() == null)){
				System.out.println("INPUT :");
				for(TypeInformation<?> inputType : this.operatorTree.get(i).getOperatorInputType())
					System.out.println(inputType+" ");
			}*/
			System.out.println("NODE :");
			System.out.println("Input Dataset - " +this.operatorTree.get(i).getOperatorInputDataSetId().get(0));
			System.out.println(this.operatorTree.get(i).getOperatorName());// .getOperatorType().name());
			System.out.println(this.operatorTree.get(i).getOperatorType().name());
			//System.out.println("OUTPUT ");
			//System.out.println(this.operatorTree.get(i).getOperatorOutputType().toString());
			if(this.operatorTree.get(i).getJUCCondition() != null)
				System.out.println(this.operatorTree.get(i).getJUCCondition().getFirstInput()+"join("+
						this.operatorTree.get(i).getJUCCondition().getSecontInput()+").where("
						+this.operatorTree.get(i).getJUCCondition().getFirstInputKeyColumns()[0]+").equalsTo("+
						this.operatorTree.get(i).getJUCCondition().getSecondInputKeyColumns()[0]+")");
		}
	}
	
}
