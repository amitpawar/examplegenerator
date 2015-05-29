package org.apache.flink.api.common.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
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
                this.opTypeShortNameToOperator.put("SOURCE"+id,operator);
                List list = ((GenericDataSourceBase) operator.getOperator()).executeOnCollections(this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("SOURCE");
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
                getUnusedExamplesForOperator(operator,"LOAD"+id);
                List list = ((SingleInputOperator)operator.getOperator()).executeOnCollections(
                        operator.getParentOperators().get(0).getOperatorOutputAsList(),null,this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("LOAD");
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
               // getUnusedExamplesForOperator(operator,"JOIN"+ctr);
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
               // getUnusedExamplesForOperator(operator,"PROJECT"+ctr);
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
              //  getUnusedExamplesForOperator(operator,"CROSS"+ctr);
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
               // getUnusedExamplesForOperator(operator,"UNION"+ctr);
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
                // getUnusedExamplesForOperator(operator,"PROJECT"+ctr);
                List list = ((SingleInputOperator)operator.getOperator()).executeOnCollections(
                        operator.getParentOperators().get(0).getOperatorOutputAsList(),null,this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("DISTINCT");
                for(Object object:list)
                    System.out.println(object);
            }


		}
	}
	
	public void upStreamPass(List<SingleOperator> operatorTree) throws Exception{
		
		for(int ctr = operatorTree.size();ctr > 0; ctr-- ){
			SingleOperator operator = operatorTree.get(ctr - 1);
			if(operator.getEquivalenceClasses() !=  null && operator.getOperatorType() == OperatorType.JOIN)
			for(EquivalenceClass eqClass : operator.getEquivalenceClasses()){
				if(eqClass.hasExample()){
                    String[] tokens = {"Test","ITShouldMatch"};
					Tuple parent1Tuple = getConstraintRecord(operator.getParentOperators().get(0),
                            new LinkedList<String>(Arrays.asList(tokens)));
                    operator.getParentOperators().get(0).getOperatorOutputAsList().add(parent1Tuple);
                    propagateConstraintRecordUpstream(operator.getParentOperators().get(0),parent1Tuple);
                    String[] secondTokens = {"ITShouldMatch","9"};
                    Tuple parent2Tuple = getConstraintRecord(operator.getParentOperators().get(1),
                            new LinkedList<String>(Arrays.asList(secondTokens)));
                    operator.getParentOperators().get(1).getOperatorOutputAsList().add(parent2Tuple);
                    propagateConstraintRecordUpstream(operator.getParentOperators().get(1),parent2Tuple);

				}
			}
				
			
		}
	}

    public void propagateConstraintRecordUpstream(SingleOperator childOperator,Tuple constraintRecord){
        for (SingleOperator parent : childOperator.getParentOperators()) {
            if (childOperator.getOperatorType() != OperatorType.LOAD) {
                while (parent.getOperatorType() != OperatorType.LOAD) {
                    parent = parent.getParentOperators().get(0);
                    parent.getOperatorOutputAsList().add(constraintRecord);
                }
               parent.getOperatorOutputAsList().add(constraintRecord);
            }
        }
    }
	
	public void pruneTuples(){
		
	}
	
	public DataSet<?> getDataSet(int id) {

		for (int i = 0; i < this.dataSources.size(); i++) {
			if (this.dataSources.get(i).getId() == id)
				return this.dataSources.get(i).getDataSet();
		}
		return null;

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Collection getCollectionForDataSet(DataSet dataset, ExecutionEnvironment env) throws Exception{
		Set collection = new HashSet();
		RemoteCollectorImpl.collectLocal(dataset, collection);
		env.execute();
		RemoteCollectorImpl.shutdownAll();
		return collection;
	}

    public Map readExampeTuples(){
        Map<String, Collection> lineageMap = new HashMap<String, Collection>();
        for(SingleOperator operator : this.operatorTree){
            lineageMap.put(operator.getOperatorName(),operator.getOperatorOutputAsList());
        }
        return  lineageMap;
    }
	
	public Map readExampleTuplesIntoCollectionTest(String outputPath)
            throws IOException, InstantiationException, IllegalAccessException {

		Map<String, Collection> lineageMap = new HashMap<String, Collection>(); //<filename, exampletuples>
		File outputDirectory = new File(outputPath);
		String line;

		for (File fileEntry : outputDirectory.listFiles()) {
			if (fileEntry.isDirectory()) {
				List<Tuple> tupleExampleSet = new ArrayList<Tuple>();
                List stringExampleSet = new ArrayList();
				for (File insideFile : fileEntry.listFiles()) {
					BufferedReader br = new BufferedReader(new FileReader(insideFile));
					while ((line = br.readLine()) != null) {
                        TypeInformation type = this.opTypeShortNameToOperator.get(fileEntry.getName()).getOperatorOutputType();
                        if(type.isTupleType())
                            tupleExampleSet.add(stringToTuple(line, type));
                        else
                            stringExampleSet.add(line);
					}
                    if(tupleExampleSet.size() > 1)
					    lineageMap.put(fileEntry.getName(), tupleExampleSet);
                    if(stringExampleSet.size() > 1)
                        lineageMap.put(fileEntry.getName(),stringExampleSet);
					br.close();
				}

			}
			else if (fileEntry.isFile()){
				List<Tuple> tupleExampleSet = new ArrayList<Tuple>();
                List stringExampleSet = new ArrayList();
				BufferedReader br = new BufferedReader(new FileReader(fileEntry));
				while ((line = br.readLine()) != null) {
                    TypeInformation type = this.opTypeShortNameToOperator.get(fileEntry.getName()).getOperatorOutputType();
                    if(type.isTupleType())
                        tupleExampleSet.add(stringToTuple(line, type));
                    else
                        stringExampleSet.add(line);
				}
                if(tupleExampleSet.size() > 1)
                    lineageMap.put(fileEntry.getName(), tupleExampleSet);
                if(stringExampleSet.size() > 1)
                    lineageMap.put(fileEntry.getName(),stringExampleSet);
                br.close();
			}
		}
		return lineageMap;
	}

    public Tuple stringToTuple(String line, TypeInformation typeInformation) throws IllegalAccessException, InstantiationException {

        Pattern toSplit = Pattern.compile("[ \t,]");
        String[] tokens = toSplit.split(line.replaceAll("[\\(\\)]",""));

        Tuple convertedTuple = drillToBasicType(typeInformation, new LinkedList<String>(Arrays.asList(tokens)));

        System.out.println(tokens);
        return convertedTuple;
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
	
	public void setEquivalenceClassesTest(Map lineageMap){
		
		Iterator operatorIt = lineageMap.keySet().iterator();
		
		while(operatorIt.hasNext()){
			String opName = operatorIt.next().toString();
			
			if(opName.contains("LOAD")){
				List loadExamples = (List)lineageMap.get(opName);
				LoadEquivalenceClasses loadEqClasses = new LoadEquivalenceClasses();
				
				if(!loadExamples.isEmpty()){
					loadEqClasses.getLoadExample().setHasExample(true);
				}
				else
					loadEqClasses.getLoadExample().setHasExample(false);
				
				SingleOperator load = this.opTypeShortNameToOperator.get(opName);
				List<EquivalenceClass> eqClass = new ArrayList<EquivalenceClass>();
				eqClass.add(loadEqClasses.getLoadExample());
				load.setEquivalenceClasses(eqClass);
				System.out.println(checkEqclassesTest(load));
			}
			
			if(opName.contains("JOIN")){
				List joinExamples = (List)lineageMap.get(opName);
				JoinEquivalenceClasses joinEqClasses = new JoinEquivalenceClasses();
				if(!joinExamples.isEmpty()){
					joinEqClasses.getJoinedExample().setHasExample(true);
				}
				else
					joinEqClasses.getJoinedExample().setHasExample(false);
				
				SingleOperator join = this.opTypeShortNameToOperator.get(opName);
				List<EquivalenceClass> eqClass = new ArrayList<EquivalenceClass>();
				eqClass.add(joinEqClasses.getJoinedExample());
				join.setEquivalenceClasses(eqClass);
				System.out.println(checkEqclassesTest(join));
			}
			
		}
	
		
	}	
	
	public void getRecordLineage(Map lineageMap){
		
		Pattern integerOnly = Pattern.compile("\\d+");
		List<Integer> opOrder = new ArrayList<Integer>();
		List<LinkedList> lineages = new ArrayList<LinkedList>();
		Set operatorSet = lineageMap.keySet();
		Iterator opIt = operatorSet.iterator();
		
		while(opIt.hasNext()){
			String operatorName = opIt.next().toString();
			Matcher makeMatch = integerOnly.matcher(operatorName);
			makeMatch.find();
			opOrder.add(Integer.parseInt(makeMatch.group()));
		}
		
		Collections.sort(opOrder, Collections.reverseOrder());
		for(int id : opOrder){
			Iterator nameIt = operatorSet.iterator();
			while(nameIt.hasNext()){
				String name = nameIt.next().toString();
				if(name.contains(Integer.toString(id))){
					List examplesForThisOperator = (List) lineageMap.get(name);
					Iterator exIt = examplesForThisOperator.iterator();
					while(exIt.hasNext()){
						LinkedList lineage = new LinkedList();
						String lineageLast = (String) exIt.next();
						lineage.add(lineageLast);
						lineages.add(lineage);
						//System.out.println(constructRecordLineage(lineageMap, opOrder, id, lineage, lineageLast));
					}
							
				}
			}
		}
		setEquivalenceClassesTest(lineageMap);
		
	}
	
	public List constructRecordLineage(Map lineageMap, List<Integer> opOrder,
			int id, LinkedList lineageGroup, String lineageLast) {

		for (int op : opOrder) {
			if (op < id) {
				Iterator keyIt = lineageMap.keySet().iterator();
				while (keyIt.hasNext()) {
					String keyName = keyIt.next().toString();
					if (keyName.contains(Integer.toString(op))) {
						List examples = (List) lineageMap.get(keyName);
						Iterator it = examples.iterator();
						while (it.hasNext()) {
							String nextLineage = it.next().toString();
							if (keyName.contains("LOAD")) {
								nextLineage = "(" + nextLineage + ")";
							}
							if (nextLineage.contains(lineageLast) || lineageLast.contains(nextLineage)) {
								//lineageGroup.add(nextLineage);
								lineageGroup.addFirst(nextLineage);
							}
						}
					}
				}
			}
		}
		return lineageGroup;
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

	public Map checkEqclassesTest(SingleOperator op){
		Map<String,Boolean> eqClassMap = new HashMap<String, Boolean>();
		for(EquivalenceClass eqClass : op.getEquivalenceClasses()){
			eqClassMap.put(eqClass.getName(), eqClass.hasExample());
		}
		return eqClassMap;
	}

	public Tuple getConstraintRecord(SingleOperator operator, List tokens) throws IllegalAccessException, InstantiationException {

        Tuple constraintRecord = drillToBasicType(operator.getOperatorOutputType(),tokens);
        return constraintRecord;
    }
	
	public DataSet getConstraintRecordsTest(SingleOperator operator) throws InstantiationException, IllegalAccessException, IOException {
		DataSet dataSetToReturn;
		TypeInformation outputType = operator.getOperatorOutputType();
		System.out.println("Operator :"+operator.getOperatorType()+" DataSetID "+operator.getOperatorInputDataSetId());
        System.out.println(outputType);
        //System.out.println(drillToBasicType(outputType));
        //dataSetToReturn = this.env.fromElements(drillToBasicType(outputType));

        return constructConstraintRecordsTest(operator);
        //return dataSetToReturn;

	}

    public DataSet constructConstraintRecordsTest(SingleOperator operator) throws IOException, InstantiationException, IllegalAccessException {

        DataSet dataSetToReturn = null;
      /*  DataSet alreadyPresentExamples = null;                              //TODO: logic similar to this
        DataSet constraintRecords = null;
        dataSetToReturn = alreadyPresentExamples.union(constraintRecords);*/
        Map<SingleOperator,DataSet> operatorToDataSetMap = new HashMap<SingleOperator, DataSet>();

        if(operator.getOperatorType() == OperatorType.JOIN){

            SingleOperator parent1 = operator.getParentOperators().get(0);
            SingleOperator parent2 = operator.getParentOperators().get(1);

            DataSet parent1Examples = parent1.getOutputExampleTuples();
            DataSet parent2Examples = parent2.getOutputExampleTuples();

            JUCCondition joinCondition = operator.getJUCCondition();
            int[] whereClause = joinCondition.getFirstInputKeyColumns();
            int[] equalsToClause = joinCondition.getSecondInputKeyColumns();

            DataSet addTup1 = null; //this.env.fromElements((drillToBasicType(parent1.getOperatorOutputType(), whereClause[0], "AMITTEST")));
            DataSet addTup2 = null; //this.env.fromElements(drillToBasicType(parent2.getOperatorOutputType(), equalsToClause[0], "AMITTEST"));

            operatorToDataSetMap.put(parent1,addTup1);
            operatorToDataSetMap.put(parent2,addTup2);

            DataSet setWithConstraintRecord1 =  parent1Examples.union(addTup1);
            DataSet setWithConstraintRecord2 = parent2Examples.union(addTup2);

            parent1.setOutputExampleTuples(setWithConstraintRecord1);
            parent2.setOutputExampleTuples(setWithConstraintRecord2);

            setWithConstraintRecord1.writeAsCsv(Config.outputPath()+"/TEST/CONSTRAINTRECORDS1",WriteMode.OVERWRITE);
            setWithConstraintRecord2.writeAsCsv(Config.outputPath()+"/TEST/CONSTRAINTRECORDS2",WriteMode.OVERWRITE);

            System.out.println();
            propagateConstraintRecordUpstreamTest(operatorToDataSetMap, operator);

        }

        return dataSetToReturn;
    }

    public void propagateConstraintRecordUpstreamTest(Map<SingleOperator, DataSet> constraintRecordList, SingleOperator operator) {

        int i = 0;
        Iterator keyIt = constraintRecordList.keySet().iterator();
        while (keyIt.hasNext()) {
            SingleOperator childOperator = (SingleOperator) keyIt.next();
            for (SingleOperator parent : childOperator.getParentOperators()) {
                DataSet unionedSet = constraintRecordList.get(childOperator);
                if (operator.getOperatorType() != OperatorType.LOAD) {
                    while (parent.getOperatorType() != OperatorType.LOAD) {
                        parent = parent.getParentOperators().get(0);
                        unionedSet = (parent.getOutputExampleTuples().union(constraintRecordList.get(childOperator)));
                        unionedSet.writeAsCsv(Config.outputPath() + "/TEST/REWRITE/" + parent.getOperatorType() + i++, WriteMode.OVERWRITE);
                    }
                    unionedSet = (parent.getOutputExampleTuples().union(constraintRecordList.get(childOperator)));
                }
                /*else
                    unionedSet = (parent.getOutputExampleTuples().union(constraintRecordList.get(childOperator)));*/

                unionedSet.writeAsCsv(Config.outputPath() + "/TEST/REWRITE/" + parent.getOperatorType() + i++, WriteMode.OVERWRITE);
            }
        }

    }

    //get unused examples from the sources of the given operator
    //todo : make it only for LOAD operator
    public Map getUnusedExamplesForOperator(SingleOperator operator, String opTypeShortName) {
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

    public void printAllDataSets(){
        for(int i = 0; i < this.operatorTree.size();i++){
            if(this.operatorTree.get(i).getOutputExampleTuples() != null)
                this.operatorTree.get(i).getOutputExampleTuples().writeAsCsv(Config.outputPath()+"/TEST/REWRITE/"+this.operatorTree.get(i).getOperatorType()+""+i,WriteMode.OVERWRITE);
        }
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
