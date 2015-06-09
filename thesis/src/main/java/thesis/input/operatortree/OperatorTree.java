package thesis.input.operatortree;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.flink.api.common.operators.base.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.api.common.operators.DualInputOperator;
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
	private List<Integer> dataSetIds;
	private int sourceCount = 0;
	private boolean isDualInputOperatorUsed = false;
    private Map<Operator,SingleOperator> operatorSingleOperatorMap = new HashMap<Operator, SingleOperator>();

	
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
		this.optimizer = new Optimizer(new DataStatistics(),new DefaultCostEstimator(), new Configuration());
		this.optimizedPlan = this.optimizer.compile(this.javaPlan);
		this.operatorTree = new ArrayList<SingleOperator>();
		this.addedNodes = new ArrayList<String>();
	}
	
	private boolean isVisited(Operator<?> operator) {
		return (this.addedNodes.contains(operator.getName()));
	}

	public List<SingleOperator> createOperatorTree() {

        this.dataSetIds = new ArrayList<Integer>();
        int sourceCtr = 0;
		for (SourcePlanNode sourceNode : this.optimizedPlan.getDataSources()) {

            List<Integer> inputDataSet = new ArrayList<Integer>();
			inputDataSet.add(this.sourceCount);
			this.dataSetIds.add(this.sourceCount);

			if (!isVisited(sourceNode.getProgramOperator())) {
				SingleOperator op = new SingleOperator();
				op.setOperatorType(OperatorType.SOURCE);
              	op.setOperator(sourceNode.getProgramOperator());
				op.setOperatorName(sourceNode.getNodeName());
				op.setOperatorOutputType(sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType());
				this.operatorTree.add(op);
                this.operatorSingleOperatorMap.put(sourceNode.getProgramOperator(), op);
				this.addedNodes.add(sourceNode.getNodeName());
				if (sourceNode.getOptimizerNode().getOutgoingConnections() != null)
					addOutgoingNodes(sourceNode.getOptimizerNode().getOutgoingConnections());
				this.sourceCount++;
			}
            sourceCtr++;
		}

		return this.operatorTree;
	}

	public void addOutgoingNodes(List<DagConnection> outgoingConnections) {
		
		for (DagConnection conn : outgoingConnections) {
			OptimizerNode node = conn.getTarget().getOptimizerNode();
			boolean isDualInputOperator = (node.getOperator() instanceof DualInputOperator)? true:false;
			
			if(isDualInputOperator && !checkOperatorTreeHasBothParentsForDualOperator((DualInputOperator) node.getOperator()))
                return;
			else
				addNode(node);
			if (node.getOutgoingConnections() != null)
				addOutgoingNodes(node.getOutgoingConnections());
		}
	}

	@SuppressWarnings("rawtypes")
	public void addNode(OptimizerNode node) {
		
		Operator<?> operator = node.getOperator();
		SingleOperator opToAdd = new SingleOperator();


       /* if (operator instanceof FlatMapOperatorBase) {
            //System.out.println("Testststs"+((FlatMapOperatorBase) operator).getInput().getClass());
             if (!isVisited(operator)) {
                opToAdd.setOperatorType(OperatorType.FLATMAP);
                addOperatorDetails(opToAdd, operator);
            }
        }*/

        if(operator instanceof SingleInputOperator) {

            if (((SingleInputOperator) operator).getInput() instanceof GenericDataSourceBase) {
                if (!isVisited(operator)) {
                    opToAdd.setOperatorType(OperatorType.LOAD);
                    addOperatorDetails(opToAdd, operator);
                }
            }
        }

		if (operator instanceof JoinOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.JOIN);
                this.isDualInputOperatorUsed = true;
				addOperatorDetails(opToAdd, operator);
                addJoinOperatorDetails((JoinOperatorBase) operator, opToAdd);
				this.sourceCount++;

			}
		}

		if (operator instanceof CrossOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.CROSS);
                this.isDualInputOperatorUsed = true;
				addOperatorDetails(opToAdd, operator);
				this.sourceCount++;
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
                this.isDualInputOperatorUsed = true;
                addOperatorDetails(opToAdd, operator);
                this.sourceCount++;
			}
		}
		
		if (operator instanceof PlanProjectOperator) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.PROJECT);

                addOperatorDetails(opToAdd,operator);
			}
		}

		if (operator instanceof GroupReduceOperatorBase) {
            if (!isVisited(operator)){
                if (operator.getName().contains("Distinct")) {
					opToAdd.setOperatorType(OperatorType.DISTINCT);

				}
                else
                    opToAdd.setOperatorType(OperatorType.GROUPREDUCE);

                if(this.isDualInputOperatorUsed)
                    addOperatorDetails(opToAdd, operator);
                else
                    addOperatorDetails(opToAdd,operator);
			}

		}

        if(operator instanceof ReduceOperatorBase){
            if(!isVisited(operator)){
                opToAdd.setOperatorType(OperatorType.REDUCE);
                if(this.isDualInputOperatorUsed)
                    addOperatorDetails(opToAdd, operator);
                else
                    addOperatorDetails(opToAdd,operator);
            }
        }
		
	}
	
	public void addOperatorDetails(SingleOperator opToAdd, Operator<?> operator){

         opToAdd.setOperatorName(operator.getName());
		opToAdd.setOperator(operator);

		opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
        opToAdd.setParentOperators(assignParentOperators(operator));

		this.operatorTree.add(opToAdd);
        this.operatorSingleOperatorMap.put(operator, opToAdd);
		this.addedNodes.add(operator.getName());
	}
	

	@SuppressWarnings("rawtypes")
	public SingleOperator addJoinOperatorDetails(JoinOperatorBase joinOperator, SingleOperator opToAdd){
			
		int[] firstInputKeys = joinOperator.getKeyColumns(InputNum.FIRST.getValue());
		int[] secondInputKeys = joinOperator.getKeyColumns(InputNum.SECOND.getValue());
		
		JUCCondition joinPred = opToAdd.new JUCCondition();

		joinPred.setFirstInputKeyColumns(firstInputKeys);
		joinPred.setSecondInputKeyColumns(secondInputKeys);
		
		opToAdd.setJUCCondition(joinPred);
		
		return opToAdd;
	
	}
	
    public List<SingleOperator> assignParentOperators(Operator operator){

        List<SingleOperator> parents = new ArrayList<SingleOperator>();
        if(operator instanceof SingleInputOperator){
            SingleOperator singleParent = this.operatorSingleOperatorMap.get(((SingleInputOperator) operator).getInput());
            parents.add(singleParent);
        }
        if(operator instanceof DualInputOperator){
            SingleOperator firstParent = this.operatorSingleOperatorMap.get(((DualInputOperator) operator).getFirstInput());
            SingleOperator secondParent = this.operatorSingleOperatorMap.get(((DualInputOperator) operator).getSecondInput());
            parents.add(0,firstParent);
            parents.add(1,secondParent);
        }
        return parents;
    }

    public boolean checkOperatorTreeHasBothParentsForDualOperator(DualInputOperator operator){
        boolean hasFirstInput = this.operatorSingleOperatorMap.get(operator.getFirstInput()) != null;
        boolean hasSecondInput = this.operatorSingleOperatorMap.get(operator.getSecondInput()) != null;
        return (hasFirstInput && hasSecondInput);
    }


	
}
