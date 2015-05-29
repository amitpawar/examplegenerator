package org.apache.flink.api.common.operators;

import java.util.*;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import thesis.algorithm.semantics.EquivalenceClass;
import thesis.algorithm.semantics.JoinEquivalenceClasses;
import thesis.algorithm.semantics.LoadEquivalenceClasses;
import thesis.examples.Config;
import thesis.input.datasources.InputDataSource;
import thesis.input.operatortree.SingleOperator;
import thesis.input.operatortree.OperatorType;
import thesis.input.operatortree.SingleOperator.JUCCondition;

public class TupleGenerator {

	private List<InputDataSource> dataSources;
	private List<SingleOperator> operatorTree;
	private Set lineageGroup = new HashSet();
	private Map <String,SingleOperator> opTypeShortNameToOperator = new HashMap<String, SingleOperator>();
	private ExecutionEnvironment env;
	private List<DataSet> lineageAdds = new ArrayList<DataSet>();
    private Map<String,DataSet> unUsedExamplesToSourceMap = new HashMap<String, DataSet>();
    private final String downstreamOutputPath =  "/home/amit/thesis/output3/TEST/downStream";
    private Map<SingleOperator,Tuple> operatorToConstraintRecordMap = new HashMap<SingleOperator, Tuple>();

	

	public List<DataSet> getLineageAdds() {
		return lineageAdds;
	}

	public void setLineageAdds(List<DataSet> lineageAdds) {
		this.lineageAdds = lineageAdds;
	}

	public TupleGenerator(List<InputDataSource> dataSources,
			List<SingleOperator> operatorTree, ExecutionEnvironment env) throws Exception {
		this.dataSources = dataSources;
		this.operatorTree = operatorTree;
		this.env = env;
		//generateTuples(this.dataSources, this.operatorTree);
		downStreamPass(this.dataSources, this.operatorTree);
        //env.execute();
		//getRecordLineage(readExampleTuplesIntoCollection(downstreamOutputPath));
        System.out.println("Lineage Map-------------------");
        System.out.println(readExampeTuples());
        setEquivalenceClasses();
        for(SingleOperator operator : this.operatorTree)
            System.out.println(checkEquivalenceClasses(operator));
        //setEquivalenceClassesTest(readExampleTuplesIntoCollection(downstreamOutputPath));
		upStreamPass(this.operatorTree);
        System.out.println("Added-------------------");
        Map addedResult = readExampeTuples();
        Iterator keyIterator = addedResult.keySet().iterator();
        while(keyIterator.hasNext()){
            Object key = keyIterator.next();
            System.out.println(key);
            System.out.println(addedResult.get(key));
        }

		//System.out.println(this.lineageGroup);
	}

	public void downStreamPass(List<InputDataSource> dataSources,List<SingleOperator> operatorTree) throws Exception{
		
		int index = 0;
		DataSet<?> dataStream = null ;
        DataSet<?>[] sources = new DataSet<?>[dataSources.size()];
        DataSet<?>[] loadSet = new DataSet<?>[dataSources.size()];
		for (int i = 0; i < dataSources.size(); i++){
			sources[i] = dataSources.get(i).getDataSet();
		}
		
		//for(SingleOperator operator : operatorTree)
		for(int ctr = 0; ctr < operatorTree.size();ctr++){
			
			SingleOperator operator = operatorTree.get(ctr);
            if(operator.getOperatorType() == OperatorType.SOURCE){
                int id = operator.getOperatorInputDataSetId().get(0);
                operator.setOutputExampleTuples(sources[id]);
                sources[id].writeAsCsv(Config.outputPath() + "/TEST/downStream/SOURCE" +id, WriteMode.OVERWRITE);
                this.opTypeShortNameToOperator.put("SOURCE "+id,operator);
                List list = ((GenericDataSourceBase) operator.getOperator()).executeOnCollections(this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("SOURCE "+operator.getOperatorName());
                for(Object object:list)
                    System.out.println(object);
            }

			if(operator.getOperatorType() == OperatorType.LOAD){
				int id = operator.getOperatorInputDataSetId().get(0); //todo introduce loop for multiple input set
				loadSet[id] = sources[id].first(2);
				operator.setOutputExampleTuples(loadSet[id]);
				loadSet[id].writeAsCsv(Config.outputPath() + "/TEST/downStream/LOAD" + ctr, WriteMode.OVERWRITE);
				this.opTypeShortNameToOperator.put("LOAD" + ctr, operator);
				this.lineageAdds.add(index++, loadSet[id]);
                getUnusedExamplesForOperatorTest(operator, "LOAD" + id);

                List inputList = new ArrayList();
                Random randomGenerator = new Random();
                inputList.add(returnRandomTuple(operator.getParentOperators().get(0).getOperatorOutputAsList(),randomGenerator));
                inputList.add(returnRandomTuple(operator.getParentOperators().get(0).getOperatorOutputAsList(),randomGenerator));

                List list = ((SingleInputOperator)operator.getOperator()).executeOnCollections(inputList,
                        null,this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("LOAD " +operator.getOperatorName());
                for(Object object:list)
                    System.out.println(object);
			}
			
			if(operator.getOperatorType() == OperatorType.JOIN){
			
				JUCCondition condition = operator.getJUCCondition();
				DataSet<?> joinResult = 
						loadSet[condition.getFirstInput()].join(loadSet[condition.getSecondInput()])
						.where(condition.getFirstInputKeyColumns())
						.equalTo(condition.getSecondInputKeyColumns());
				operator.setOutputExampleTuples(joinResult);
				joinResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/JOIN"+ctr,WriteMode.OVERWRITE);
				this.opTypeShortNameToOperator.put("JOIN" + ctr, operator);
				dataStream = joinResult;
				this.lineageAdds.add(index++, joinResult);
               // getUnusedExamplesForOperatorTest(operator,"JOIN"+ctr);
                List list = ((DualInputOperator)operator.getOperator()).executeOnCollections(
                        operator.getParentOperators().get(0).getOperatorOutputAsList(),
                        operator.getParentOperators().get(1).getOperatorOutputAsList(),null,this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("JOIN");
                for(Object object:list)
                    System.out.println(object);
			}
			
			if(operator.getOperatorType() == OperatorType.PROJECT){
			
				DataSet projResult = dataStream.project(operator.getProjectColumns());
				operator.setOutputExampleTuples(projResult);
				projResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/PROJECT"+ctr,WriteMode.OVERWRITE);
				this.opTypeShortNameToOperator.put("PROJECT" + ctr, operator);
				dataStream = projResult;
				this.lineageAdds.add(index++, projResult);
               // getUnusedExamplesForOperatorTest(operator,"PROJECT"+ctr);
                List list = ((SingleInputOperator)operator.getOperator()).executeOnCollections(
                        operator.getParentOperators().get(0).getOperatorOutputAsList(),null,this.env.getConfig());

                operator.setOperatorOutputAsList(list);
                System.out.println("PROJECT");
                for(Object object:list)
                    System.out.println(object);
			}
			
			if(operator.getOperatorType() == OperatorType.CROSS){
			
				JUCCondition condition = operator.getJUCCondition();
				DataSet<?> crossResult = 
						loadSet[condition.getFirstInput()].cross(loadSet[condition.getSecondInput()]);
				operator.setOutputExampleTuples(crossResult);
				crossResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/CROSS"+ctr,WriteMode.OVERWRITE);
				this.opTypeShortNameToOperator.put("CROSS" + ctr, operator);
				dataStream = crossResult;
				this.lineageAdds.add(index++, crossResult);
              //  getUnusedExamplesForOperatorTest(operator,"CROSS"+ctr);
			}
			
			if(operator.getOperatorType() == OperatorType.UNION){
				JUCCondition condition = operator.getJUCCondition();
				DataSet firstInput = loadSet[condition.getFirstInput()];
				DataSet secondInput = loadSet[condition.getSecondInput()];
				DataSet<?> unionResult = firstInput.union (secondInput);
				operator.setOutputExampleTuples(unionResult);
				unionResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/UNION"+ctr,WriteMode.OVERWRITE);
				this.opTypeShortNameToOperator.put("UNION" + ctr, operator);
				dataStream = unionResult;
				this.lineageAdds.add(index++, unionResult);
               // getUnusedExamplesForOperatorTest(operator,"UNION"+ctr);
                List list = ((DualInputOperator)operator.getOperator()).executeOnCollections(
                        operator.getParentOperators().get(0).getOperatorOutputAsList(),
                        operator.getParentOperators().get(1).getOperatorOutputAsList(),null,this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("UNION");
                for(Object object:list)
                    System.out.println(object);
			}

            if(operator.getOperatorType() == OperatorType.DISTINCT){

                DataSet distinctResult = loadSet[operator.getOperatorInputDataSetId().get(0)].distinct();
                operator.setOutputExampleTuples(distinctResult);
                //distinctResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/DISTINCT"+ctr,WriteMode.OVERWRITE);
                this.opTypeShortNameToOperator.put("DISTINCT" + ctr, operator);
                dataStream = distinctResult;
                this.lineageAdds.add(index++, distinctResult);
                // getUnusedExamplesForOperatorTest(operator,"PROJECT"+ctr);
                List list = ((SingleInputOperator)operator.getOperator()).executeOnCollections(
                        operator.getParentOperators().get(0).getOperatorOutputAsList(),null,this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("DISTINCT "+operator.getOperatorName());
                for(Object object:list)
                    System.out.println(object);
            }


		}
	}

    public Object returnRandomTuple(List parentOutput, Random randomGenerator){
        int index = randomGenerator.nextInt(parentOutput.size());
        return parentOutput.get(index);
    }
	
	public void upStreamPass(List<SingleOperator> operatorTree) throws Exception{
		
		for(int ctr = operatorTree.size();ctr > 0; ctr-- ){
			SingleOperator operator = operatorTree.get(ctr - 1);
            //todo logic for not including source
			if(operator.getEquivalenceClasses() !=  null && operator.getOperatorType() == OperatorType.JOIN) {
                for (EquivalenceClass eqClass : operator.getEquivalenceClasses()) {
                    if (eqClass.hasExample()||!eqClass.hasExample()) {
                        String[] tokens = {"Test", "ITShouldMatch"};
                        Tuple parent1Tuple = getConstraintRecord(operator.getParentOperators().get(0),
                                new LinkedList<String>(Arrays.asList(tokens)));
                        operator.getParentOperators().get(0).getOperatorOutputAsList().add(parent1Tuple);
                        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(0),parent1Tuple);
                        propagateConstraintRecordUpstream(operator.getParentOperators().get(0), parent1Tuple);

                        String[] secondTokens = {"ITShouldMatch", "9"};
                        Tuple parent2Tuple = getConstraintRecord(operator.getParentOperators().get(1),
                                new LinkedList<String>(Arrays.asList(secondTokens)));
                        operator.getParentOperators().get(1).getOperatorOutputAsList().add(parent2Tuple);
                        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(1),parent2Tuple);
                        propagateConstraintRecordUpstream(operator.getParentOperators().get(1), parent2Tuple);

                    }
                }

            }

		}
	}

    public void changeConstraintRecordToConcreteRecord(SingleOperator child, SingleOperator operatorWithEmptyEqClass) throws Exception {

        Map<SingleOperator, List> loadOperatorWithUnUsedExamples = new HashMap<SingleOperator, List>();
        for (SingleOperator parent : child.getParentOperators()) {
            //convert only if its a leaf operator
            if (parent.getOperator() instanceof GenericDataSourceBase) {
                System.out.println("UNUSED-----");
                List unUsedExamplesAtLeaf = getUnusedExamplesFromBaseTable(parent, child, child.getOperatorOutputAsList());
                loadOperatorWithUnUsedExamples.put(parent,unUsedExamplesAtLeaf);
            }
        }
        if(operatorWithEmptyEqClass.getOperatorType() == OperatorType.JOIN){


        }

    }

    public List getUnusedExamplesFromBaseTable(SingleOperator baseOperator, SingleOperator leafOperator, List usedExamples) throws Exception {
        List allExamples = baseOperator.getOperatorOutputAsList();
        List allExamplesAtLeaf = ((SingleInputOperator)leafOperator.getOperator())
                .executeOnCollections(allExamples, null, this.env.getConfig());


        allExamplesAtLeaf.removeAll(usedExamples);

        return allExamplesAtLeaf;

    }

    public void propagateConstraintRecordUpstream(SingleOperator childOperator,Tuple constraintRecord) throws Exception {
        for (SingleOperator parent : childOperator.getParentOperators()) {
            if (childOperator.getOperatorType() != OperatorType.LOAD) {
                while (parent.getOperatorType() != OperatorType.LOAD) {
                    parent = parent.getParentOperators().get(0);
                    parent.getOperatorOutputAsList().add(constraintRecord);
                    this.operatorToConstraintRecordMap.put(parent,constraintRecord);
                }
                parent.getOperatorOutputAsList().add(constraintRecord);
                this.operatorToConstraintRecordMap.put(parent,constraintRecord);
                //parent is LOAD
                changeConstraintRecordToConcreteRecord(parent, childOperator);
            }
        }
    }
	
	public void pruneTuples(){
		
	}




    public Map readExampeTuples(){
        Map<String, Collection> lineageMap = new HashMap<String, Collection>();
        for(SingleOperator operator : this.operatorTree){
            lineageMap.put(operator.getOperatorName(),operator.getOperatorOutputAsList());
        }
        return  lineageMap;
    }
	
    public void setEquivalenceClasses(){
        for(SingleOperator operator : this.operatorTree){

            if(operator.getOperatorType() == OperatorType.LOAD){
                List loadExamples = operator.getOperatorOutputAsList();
                LoadEquivalenceClasses loadEquivalenceClass = new LoadEquivalenceClasses();

                if(!loadExamples.isEmpty()){
                    loadEquivalenceClass.getLoadExample().setHasExample(true);
                }
                else
                    loadEquivalenceClass.getLoadExample().setHasExample(false);

                List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
                equivalenceClasses.add(loadEquivalenceClass.getLoadExample());
                operator.setEquivalenceClasses(equivalenceClasses);
            }

            if(operator.getOperatorType() == OperatorType.JOIN){
                List joinExamples = operator.getOperatorOutputAsList();
                JoinEquivalenceClasses joinEquivalenceClass = new JoinEquivalenceClasses();
                if(!joinExamples.isEmpty()){
                    joinEquivalenceClass.getJoinedExample().setHasExample(true);
                }
                else
                    joinEquivalenceClass.getJoinedExample().setHasExample(false);


                List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
                equivalenceClasses.add(joinEquivalenceClass.getJoinedExample());
                operator.setEquivalenceClasses(equivalenceClasses);

            }
        }
    }
	


	


    public String checkEquivalenceClasses(SingleOperator operator){
        String returnString = "";
        if(operator.getEquivalenceClasses() !=  null) {
            for (EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()) {
                if (equivalenceClass.hasExample())
                    returnString = returnString + " " + equivalenceClass.getName() + " " + equivalenceClass.hasExample();
                else
                    returnString = returnString + " " + equivalenceClass.getName() + " false";
            }
        }
        else
            returnString = returnString+" "+operator.getOperatorName()+" eq not set";
        return returnString;
    }


	public Tuple getConstraintRecord(SingleOperator operator, List tokens) throws IllegalAccessException, InstantiationException {

        Tuple constraintRecord = drillToBasicType(operator.getOperatorOutputType(),tokens);
        return constraintRecord;
    }
	


    //get unused examples from the sources of the given operator
    //todo : make it only for LOAD operator
    public Map getUnusedExamplesForOperatorTest(SingleOperator operator, String opTypeShortName) {
        int j = 0;
        int sourceId = -1;

        if (operator.getOperatorType() != OperatorType.SOURCE) {
            for (SingleOperator parent : operator.getParentOperators()) {
                DataSet usedLoadExamples;
                if (operator.getOperatorType() != OperatorType.LOAD) {
                    while (parent.getOperatorType() != OperatorType.LOAD) {
                        parent = parent.getParentOperators().get(0);
                    }
                usedLoadExamples = parent.getOutputExampleTuples();
                }
                else
                usedLoadExamples = operator.getOutputExampleTuples();

                while (parent.getOperatorType() != OperatorType.SOURCE) {
                    parent = parent.getParentOperators().get(0);
                    sourceId = parent.getOperatorInputDataSetId().get(0);
                }
                DataSet mainSourceExamples = parent.getOutputExampleTuples();
                DataSet unUsedExamples = mainSourceExamples.filter(new TupleFilter()).withBroadcastSet(usedLoadExamples, "filterset");
                this.unUsedExamplesToSourceMap.put(opTypeShortName, unUsedExamples);
                if(operator.getParentOperators().size() > 1)
                    unUsedExamples.writeAsCsv(Config.outputPath() + "/TEST/UNUSED/" + opTypeShortName +j++, WriteMode.OVERWRITE);
                else
                    unUsedExamples.writeAsCsv(Config.outputPath() + "/TEST/UNUSED/" + opTypeShortName +"/", WriteMode.OVERWRITE);

            }
        }
        return this.unUsedExamplesToSourceMap;
    }

    //Todo : check for all types
    public Tuple drillToBasicType(TypeInformation typeInformation, List tokens) throws IllegalAccessException, InstantiationException {
        Tuple testTuple = (Tuple) typeInformation.getTypeClass().newInstance();

        for (int ctr = 0; ctr < typeInformation.getArity(); ctr++)

            if (((CompositeType) typeInformation).getTypeAt(ctr).isTupleType())
                testTuple.setField(drillToBasicType(((CompositeType) typeInformation).getTypeAt(ctr), tokens), ctr);

            else {
                String name = ((CompositeType) typeInformation).getTypeAt(ctr).toString();//todo add tuple field from source

                testTuple.setField(tokens.get(0),ctr);
                tokens.remove(tokens.get(0));
                /*if (name.equalsIgnoreCase("String"))
                    testTuple.setField(name, ctr);
                if (name.equalsIgnoreCase("Long")) {
                    long someValue = -9999;
                    testTuple.setField(someValue, ctr);

                }*/
            }

        return testTuple;


    }


	
	/////////////////////////////
	
	public static class TupleFilter extends RichFilterFunction{

		private Collection<?> sampleSet;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			this.sampleSet = getRuntimeContext().getBroadcastVariable("filterset");
		}
		
		@Override
		public boolean filter(Object arg0) throws Exception {
			return !sampleSet.contains(arg0);
		}
	}


}
