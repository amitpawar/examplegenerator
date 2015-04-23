package thesis.input.operatortree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.operators.base.FlatMapOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.translation.JavaPlan;
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
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import thesis.input.operatortree.SingleOperator.JoinCondition;

public class OperatorTree {

	private JavaPlan javaPlan;
	private Optimizer optimizer;
	private OptimizedPlan optimizedPlan;
	private List<SingleOperator> operatorTree;
	private List<String> addedNodes;
	
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

	public OperatorTree(ExecutionEnvironment env) {
		this.javaPlan = env.createProgramPlan();
		this.optimizer = new Optimizer(new DataStatistics(),
				new DefaultCostEstimator());
		this.optimizedPlan = this.optimizer.compile(this.javaPlan);
		this.operatorTree = new ArrayList<SingleOperator>();
		this.addedNodes = new ArrayList<String>();
	}

	private boolean isVisited(Operator<?> operator) {
		return (this.addedNodes.contains(operator.getName()));
	}

	public List<SingleOperator> createOperatorTree() {
		
		for (SourcePlanNode sourceNode : this.optimizedPlan.getDataSources()) {
		
			if (!isVisited(sourceNode.getProgramOperator())) {
				SingleOperator op = new SingleOperator();
				op.setOperatorType(OperatorType.LOAD);
				op.setOperator(sourceNode.getProgramOperator());
				op.setOperatorName(sourceNode.getNodeName());
				op.setOperatorOutputType(sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType());
				this.operatorTree.add(op);
				this.addedNodes.add(sourceNode.getNodeName());
				if (sourceNode.getOptimizerNode().getOutgoingConnections() != null)
					addOutgoingNodes(sourceNode.getOptimizerNode().getOutgoingConnections());
			}
		}
		
		for (int i = 0; i < this.operatorTree.size(); i++) {
			if(!(this.operatorTree.get(i).getOperatorInputType() == null)){
				System.out.println("INPUT :");
				for(TypeInformation<?> inputType : this.operatorTree.get(i).getOperatorInputType())
					System.out.println(inputType+" ");
			}
			System.out.println("NODE :");
			System.out.println(this.operatorTree.get(i).getOperatorName());// .getOperatorType().name());
			System.out.println(this.operatorTree.get(i).getOperatorType().name());
			System.out.println("OUTPUT ");
			System.out.println(this.operatorTree.get(i).getOperatorOutputType().toString());
			if(this.operatorTree.get(i).getJoinCondition() != null)
				System.out.println(this.operatorTree.get(i).getJoinCondition().getFirstInput()+"join("+
						this.operatorTree.get(i).getJoinCondition().getSecontInput()+").where("
						+this.operatorTree.get(i).getJoinCondition().getFirstInputKeyColumns()[0]+").equalsTo("+
						this.operatorTree.get(i).getJoinCondition().getSecondInputKeyColumns()[0]+")");
		}
		
		return this.operatorTree;
	}

	public void addOutgoingNodes(List<DagConnection> outgoingConnections) {
		for (DagConnection conn : outgoingConnections) {
			SingleOperator op = new SingleOperator();
			OptimizerNode node = conn.getTarget().getOptimizerNode();
			
			
		/*	op.setOperatorName(node.getOperator().toString());
		    op.setOperator(node.getOperator());
			op.setOperatorInputType(addInputTypes(node.getOperator()));
			op.setOperatorOutputType(node.getOperator().getOperatorInfo().getOutputType());
			this.operators.add(op);*/
			 
			addNode(node);
			if (node.getOutgoingConnections() != null)
				addOutgoingNodes(node.getOutgoingConnections());
		}
	}

	@SuppressWarnings("rawtypes")
	public void addNode(OptimizerNode node) {
		
		Operator<?> operator = node.getOperator();
		SingleOperator opToAdd = new SingleOperator();
		
		if(operator instanceof FlatMapOperatorBase){
			//System.out.println("Testststs"+((FlatMapOperatorBase) operator).getInput().getClass());
			if(((FlatMapOperatorBase) operator).getInput() instanceof GenericDataSourceBase){
				System.out.println("Yes this is flatmap after Load/DataSource "+operator.getOperatorInfo().getOutputType());
				//System.out.println(((FlatMapOperatorBase) operator).
				System.out.println(((FlatMapOperatorBase) operator).getOperatorInfo().getInputType());
			}
		}
		
		if (operator instanceof JoinOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.JOIN);
				SingleOperator opToAddWithJoinPred = addJoinOperatorDetails((JoinOperatorBase) operator, opToAdd);
				addOperatorDetails(opToAddWithJoinPred, operator);
				
			}
		}

		if (operator instanceof CrossOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.CROSS);
				addOperatorDetails(opToAdd, operator);
			}
		}

		if (operator instanceof FilterOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.FILTER);
				addOperatorDetails(opToAdd, operator);
			}
		}
		
		if (operator instanceof Union<?>) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.UNION);
				addOperatorDetails(opToAdd, operator);
			}
		}
		
		if (operator instanceof PlanProjectOperator) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.PROJECT);
				addOperatorDetails(opToAdd, operator);
			}
		}

		if (operator instanceof GroupReduceOperatorBase) {
			if (operator.getName().contains("Distinct")) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.DISTINCT);
					addOperatorDetails(opToAdd, operator);
				}
			}
		}
		
	}
	
	public void addOperatorDetails(SingleOperator opToAdd, Operator<?> operator){
		opToAdd.setOperatorName(operator.getName());
		opToAdd.setOperator(operator);
		opToAdd.setOperatorInputType(addInputTypes(operator));
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
		
		JoinCondition joinPred = opToAdd.new JoinCondition();
		
		joinPred.setFirstInput(InputNum.FIRST.getValue());
		joinPred.setSecontInput(InputNum.SECOND.getValue());
		joinPred.setFirstInputKeyColumns(firstInputKeys);
		joinPred.setSecondInputKeyColumns(secondInputKeys);
		
		opToAdd.setJoinCondition(joinPred);
		
		return opToAdd;
	
	}
	
}
