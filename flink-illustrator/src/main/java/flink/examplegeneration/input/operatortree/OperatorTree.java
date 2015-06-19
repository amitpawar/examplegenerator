package flink.examplegeneration.input.operatortree;

import java.util.*;

import org.apache.flink.api.common.operators.*;
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
import org.apache.flink.configuration.Configuration;

import flink.examplegeneration.input.operatortree.SingleOperator.JUCCondition;

/**
 * The class that creates operator tree for the given Flink job
 */
public class OperatorTree {

	private JavaPlan javaPlan;
	private Optimizer optimizer;
	private OptimizedPlan optimizedPlan;
	private List<SingleOperator> operatorTree;
	private List<Operator> addedNodes;
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

    /**
     * Creates  new instance of the operator tree.
     * @param env {@link org.apache.flink.api.java.ExecutionEnvironment} of the Flink job
     */
    public OperatorTree(ExecutionEnvironment env) {
		this.javaPlan = env.createProgramPlan();
		this.optimizer = new Optimizer(new DataStatistics(),new DefaultCostEstimator(), new Configuration());
		this.optimizedPlan = this.optimizer.compile(this.javaPlan);
		this.operatorTree = new ArrayList<SingleOperator>();
		this.addedNodes = new ArrayList<Operator>();
	}

	public OperatorTree(JavaPlan plan){
        this.javaPlan = plan;
        this.optimizer = new Optimizer(new DataStatistics(),new DefaultCostEstimator(), new Configuration());
        this.optimizedPlan = this.optimizer.compile(this.javaPlan);
        this.operatorTree = new ArrayList<SingleOperator>();
        this.addedNodes = new ArrayList<Operator>();
    }
	
	private boolean isVisited(Operator<?> operator) {
		return (this.addedNodes.contains(operator));
	}

    /**
     * Creates the operator tree by traversing from source to sink
     * @return The operator tree
     */
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
				this.addedNodes.add(sourceNode.getProgramOperator());
				if (sourceNode.getOptimizerNode().getOutgoingConnections() != null)
					addOutgoingNodes(sourceNode.getOptimizerNode().getOutgoingConnections());
				this.sourceCount++;
			}
            sourceCtr++;
		}

		return this.operatorTree;
	}

    /**
     * Adds outgoing node for the given operator
     * @param outgoingConnections The outgoing connections of the given operators
     */
    private void addOutgoingNodes(List<DagConnection> outgoingConnections) {
		
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

    /**
     * Adds single node operator
     * @param node The {@link org.apache.flink.optimizer.dag.OptimizerNode} object
     */
	@SuppressWarnings("rawtypes")
	private void addNode(OptimizerNode node) {
		
		Operator<?> operator = node.getOperator();
		SingleOperator opToAdd = new SingleOperator();

        if(operator instanceof SingleInputOperator) {

            if (((SingleInputOperator) operator).getInput() instanceof GenericDataSourceBase) {
                if (!isVisited(operator)) {
                    opToAdd.setOperatorType(OperatorType.LOAD);
                    addOperatorDetails(opToAdd, operator);
                }
            }
			else{
				if (operator instanceof FlatMapOperatorBase) {
					//System.out.println("Testststs"+((FlatMapOperatorBase) operator).getInput().getClass());
					if (!isVisited(operator)) {
                        opToAdd.setOperatorType(OperatorType.FLATMAP);
						addOperatorDetails(opToAdd, operator);
					}
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

				addOperatorDetails(opToAdd, operator);
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

    /**
     * Adds the necessary operator details
     * @param opToAdd The operator to add
     * @param operator The {@link org.apache.flink.api.common.operators} instance
     */
    private void addOperatorDetails(SingleOperator opToAdd, Operator<?> operator){

        opToAdd.setOperatorName(operator.getName());
		opToAdd.setOperator(operator);

		opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
        opToAdd.setParentOperators(getParentOperators(operator));

        if(operator instanceof SingleInputOperator){
            SemanticProperties semanticProperties = ((SingleInputOperator) operator).getSemanticProperties();
            if(semanticProperties != null)
                opToAdd.setSemanticProperties(semanticProperties);
        }
        if(operator instanceof DualInputOperator){
            SemanticProperties semanticProperties = ((DualInputOperator) operator).getSemanticProperties();

            if(semanticProperties != null)
                opToAdd.setSemanticProperties(semanticProperties);
        }



		this.operatorTree.add(opToAdd);
        this.operatorSingleOperatorMap.put(operator, opToAdd);
		this.addedNodes.add(operator);
	}


    /**
     * Adds the join details for the Join operator
     * @param joinOperator The JOIN operator
     * @param opToAdd The {@link org.apache.flink.api.common.operators.base.JoinOperatorBase} instance
     * @return The JOIN operator object with details added
     */
	@SuppressWarnings("rawtypes")
	private SingleOperator addJoinOperatorDetails(JoinOperatorBase joinOperator, SingleOperator opToAdd){
			
		int[] firstInputKeys = joinOperator.getKeyColumns(InputNum.FIRST.getValue());
		int[] secondInputKeys = joinOperator.getKeyColumns(InputNum.SECOND.getValue());
		
		JUCCondition joinPred = opToAdd.new JUCCondition();

		joinPred.setFirstInputKeyColumns(firstInputKeys);
		joinPred.setSecondInputKeyColumns(secondInputKeys);
		
		opToAdd.setJUCCondition(joinPred);
		
		return opToAdd;
	
	}

    /**
     * Gets parent operators to a given operator
     * @param operator The operator whose parents are searched.
     * @return The parent operators list
     */
    private List<SingleOperator> getParentOperators(Operator operator){

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

    /**
     * Checks whether all parents for the given operator is in the operator tree
     * @param operator The operator under consideration
     * @return true if they are present
     */
    private boolean checkOperatorTreeHasBothParentsForDualOperator(DualInputOperator operator){
        boolean hasFirstInput = this.operatorSingleOperatorMap.get(operator.getFirstInput()) != null;
        boolean hasSecondInput = this.operatorSingleOperatorMap.get(operator.getSecondInput()) != null;
        return (hasFirstInput && hasSecondInput);
    }


	
}
