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
	private List<InputDataSource> dataSets;
    private List<Integer> dataSetIds;
	private int sourceCount = 0;
	private boolean isDualInputOperatorUsed = false;
    private Map<Operator,SingleOperator> operatorSingleOperatorMap = new HashMap<Operator, SingleOperator>();
    private Map<Integer,List<Integer>> dataSetIdMap = new HashMap<Integer, List<Integer>>();
    private int dualOperatorOutputCtr;
	
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
		this.dualOperatorOutputCtr = dataSets.size();
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
                op.setOutputDataSetId(this.dataSets.get(sourceCtr).getId());
				op.setOperatorInputDataSetId(inputDataSet);
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
		//displayItems();
		
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
				addJUCDetails(opToAdd);
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
                addJUCDetails(opToAdd);
				this.sourceCount++;
			}
		}
		
		if (operator instanceof PlanProjectOperator) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.PROJECT);
				opToAdd.setProjectColumns(getProjectArray(operator.getName()));
                if(this.isDualInputOperatorUsed)
                    addOperatorDetails(opToAdd, operator);
                else
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

        List<Integer> inputDataSetIds = new ArrayList<Integer>();
        opToAdd.setOperatorName(operator.getName());
		opToAdd.setOperator(operator);
		opToAdd.setOperatorInputType(addInputTypes(operator));
		opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
        opToAdd.setParentOperators(assignParentOperators(operator));
        for(SingleOperator parent : opToAdd.getParentOperators())
            inputDataSetIds.add(parent.getOutputDataSetId());
        opToAdd.setOperatorInputDataSetId(inputDataSetIds);
        if(operator instanceof SingleInputOperator){
            opToAdd.setOutputDataSetId(opToAdd.getParentOperators().get(0).getOutputDataSetId());
        }
        if(operator instanceof DualInputOperator){
            opToAdd.setOutputDataSetId(this.dualOperatorOutputCtr);
            this.dataSetIdMap.put(this.dualOperatorOutputCtr,inputDataSetIds);
            this.dualOperatorOutputCtr++;
        }
		this.operatorTree.add(opToAdd);
        this.operatorSingleOperatorMap.put(operator, opToAdd);
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
		
		//joinPred.setFirstInput(InputNum.FIRST.getValue());
        joinPred.setFirstInput(opToAdd.getParentOperators().get(0).getOutputDataSetId());
		//joinPred.setSecondInput(InputNum.SECOND.getValue());
        joinPred.setSecondInput(opToAdd.getParentOperators().get(1).getOutputDataSetId());
		joinPred.setFirstInputKeyColumns(firstInputKeys);
		joinPred.setSecondInputKeyColumns(secondInputKeys);
		
		opToAdd.setJUCCondition(joinPred);
		
		return opToAdd;
	
	}
	
	public SingleOperator addJUCDetails(SingleOperator opToAdd){
		JUCCondition condition = opToAdd.new JUCCondition();
		//condition.setFirstInput(InputNum.FIRST.getValue());
		condition.setFirstInput(opToAdd.getParentOperators().get(0).getOutputDataSetId());
		//condition.setSecondInput(InputNum.SECOND.getValue());
        condition.setSecondInput(opToAdd.getParentOperators().get(1).getOutputDataSetId());
		condition.setOperatorType(opToAdd.getOperatorType());
		opToAdd.setJUCCondition(condition);
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

	public void displayItems(){
		
		for (int i = 0; i < this.operatorTree.size(); i++) {
		/*	if(!(this.operatorTree.get(i).getOperatorInputType() == null)){
				System.out.println("INPUT :");
				for(TypeInformation<?> inputType : this.operatorTree.get(i).getOperatorInputType())
					System.out.println(inputType+" ");
			}*/
			System.out.println("NODE :"+this.operatorTree.get(i).getOperatorType());
            if(this.operatorTree.get(i).getParentOperators()!= null){
                int ctr = 1;
                for(SingleOperator parent : this.operatorTree.get(i).getParentOperators()){
                    System.out.println("Parent - "+ctr+++" "+parent.getOperatorName());
                }
            }
			System.out.println("Input Dataset - " +this.operatorTree.get(i).getOperatorInputDataSetId());
			System.out.println(this.operatorTree.get(i).getOperatorName());// .getOperatorType().name());
			//System.out.println("OUTPUT ");
			//System.out.println(this.operatorTree.get(i).getOperatorOutputType().toString());
			if(this.operatorTree.get(i).getJUCCondition() != null)
			{
				if(this.operatorTree.get(i).getJUCCondition().getFirstInputKeyColumns()!= null)
				    System.out.println(this.operatorTree.get(i).getJUCCondition().getFirstInput()+"join("+
						this.operatorTree.get(i).getJUCCondition().getSecondInput()+").where("
						+this.operatorTree.get(i).getJUCCondition().getFirstInputKeyColumns()[0]+").equalsTo("+
						this.operatorTree.get(i).getJUCCondition().getSecondInputKeyColumns()[0]+")");
                else
                    System.out.println(this.operatorTree.get(i).getJUCCondition().getFirstInput()+" "+
                    this.operatorTree.get(i).getJUCCondition().getOperatorType()+" "+
                    this.operatorTree.get(i).getJUCCondition().getSecondInput());
			}
            System.out.println("Output DataSet : " + this.operatorTree.get(i).getOutputDataSetId());
		}
	}
	
}
