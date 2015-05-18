package thesis.algorithm.logic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.io.RemoteCollectorOutputFormat;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import scala.Array;
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
	private Map <String,SingleOperator> opTypeToOperator = new HashMap<String, SingleOperator>();
	private ExecutionEnvironment env;
	private List<DataSet> lineageAdds = new ArrayList<DataSet>();
	

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
		getRecordLineage(readDownstreamExamplesIntoCollection("/home/amit/thesis/output/TEST/downStream"));
		upStreamPass(this.operatorTree);
		System.out.println(this.lineageGroup);
	}

	@SuppressWarnings("unchecked")
	public void generateTuplesTest(ExecutionEnvironment env,
			List<DataSet<?>> dataSources, List<SingleOperator> operatorTree)
			throws Exception {

		for (SingleOperator operator : operatorTree) {
			if (operator.getOperatorType() == OperatorType.LOAD) {
				dataSources.get(1).getType();
				DataSet dataset =  dataSources.get(1).first(2);

				Set coll = new HashSet();

				RemoteCollectorImpl.collectLocal(dataset, coll);
				// env.execute();

				Tuple2<?, ?> addtup = new Tuple2<String, String>("Test", "Test");
				coll.add(addtup);
				DataSet<Tuple2<?, ?>> newDS = env.fromCollection(coll);
				//newDS.print();
				env.execute();
				System.out.println("Set " + coll);
				RemoteCollectorImpl.shutdownAll();
				downStreamPass(this.dataSources, this.operatorTree);

				if (dataSources.get(1).getType().getTypeClass().newInstance() instanceof Tuple2) {
					// System.out.println(dataSources.get(1).getType().getTypeClass().newInstance());
				}

				switch (dataSources.get(1).getType().getTotalFields()) {

				case 1:
					DataSet<Tuple1<?>> dataSet1 = (DataSet<Tuple1<?>>) dataSources
							.get(1);
					Set<Tuple1<?>> coll1 = new HashSet<Tuple1<?>>();
					RemoteCollectorImpl.collectLocal(dataSet1, coll1);
					DataSet<Tuple1<?>> fromColl = env.fromCollection(coll1);
				}
			}
		}

	}

	public void generateTuples(List<InputDataSource> dataSources,
			List<SingleOperator> operatorTree) throws Exception {

		int ctr = 0;
		int ctr1 = 0;
		int ctr2 = 0;
		for (SingleOperator operator : operatorTree) {

			List<DataSet<?>> dataSets = new ArrayList<DataSet<?>>();
			if (operator.getOperatorType() == OperatorType.LOAD) {
				for (int i = 0; i < operator.getOperatorInputDataSetId().size(); i++) {
					dataSets.add(getDataSet(operator
							.getOperatorInputDataSetId().get(i)));
					DataSet<?> mainSet = dataSets.get(i);
					mainSet.writeAsCsv(Config.outputPath()+"/TEST/Main/"+ctr1++);
					DataSet<?> sampleSet = dataSets.get(i).first(2);// .writeAsCsv(Config.outputPath()+"/TEST/LOAD"+ctr++,WriteMode.OVERWRITE);
					sampleSet.writeAsCsv(Config.outputPath()+"/TEST/LOAD"+ctr++,WriteMode.OVERWRITE);										
					operator.setExampleTuples(sampleSet);
					DataSet<?> filterSet = mainSet.filter(new TupleFilter())
							.withBroadcastSet(sampleSet, "filterset");
					
					filterSet.writeAsCsv(Config.outputPath()+"/TEST/Filter"+ctr2++);
				}

			}

		}
	}

	public void downStreamPass(List<InputDataSource> dataSources,List<SingleOperator> operatorTree) throws Exception{
		
		int index = 0;
		DataSet<?> dataStream = null ;
		DataSet<?>[] sources = new DataSet<?>[dataSources.size()];
		for (int i = 0; i < dataSources.size(); i++){
			sources[i] = dataSources.get(i).getDataSet();
			//sources[i].writeAsCsv(Config.outputPath()+"/TEST/downStream/LOAD"+i,WriteMode.OVERWRITE);
		}
		
		//for(SingleOperator operator : operatorTree)
		for(int ctr = 0; ctr < operatorTree.size();ctr++){
			
			SingleOperator operator = operatorTree.get(ctr);
			if(operator.getOperatorType() == OperatorType.LOAD){
				int id = operator.getOperatorInputDataSetId().get(0);
				sources[id] = sources[id].first(2);
				operator.setExampleTuples(sources[id]);
				sources[id].writeAsCsv(Config.outputPath()+"/TEST/downStream/LOAD"+ctr,WriteMode.OVERWRITE);
				this.opTypeToOperator.put("LOAD"+ctr, operator);
				this.lineageAdds.add(index++, sources[id]);
			}
			
			if(operator.getOperatorType() == OperatorType.JOIN){
			
				JUCCondition condition = operator.getJUCCondition();
				DataSet<?> joinResult = 
						sources[condition.getFirstInput()].join(sources[condition.getSecondInput()])
						.where(condition.getFirstInputKeyColumns())
						.equalTo(condition.getSecondInputKeyColumns());
				operator.setExampleTuples(joinResult);
				joinResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/JOIN"+ctr,WriteMode.OVERWRITE);
				this.opTypeToOperator.put("JOIN"+ctr, operator);
				dataStream = joinResult;
				this.lineageAdds.add(index++, joinResult);
			}
			
			if(operator.getOperatorType() == OperatorType.PROJECT){
			
				DataSet<?> projResult = dataStream.project(operator.getProjectColumns());
				operator.setExampleTuples(projResult);
				projResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/PROJECT"+ctr,WriteMode.OVERWRITE);
				this.opTypeToOperator.put("PROJECT"+ctr, operator);
				dataStream = projResult;
				this.lineageAdds.add(index++, projResult);
			}
			
			if(operator.getOperatorType() == OperatorType.CROSS){
			
				JUCCondition condition = operator.getJUCCondition();
				DataSet<?> crossResult = 
						sources[condition.getFirstInput()].cross(sources[condition.getSecondInput()]);
				operator.setExampleTuples(crossResult);
				crossResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/CROSS"+ctr,WriteMode.OVERWRITE);
				this.opTypeToOperator.put("CROSS"+ctr, operator);
				dataStream = crossResult;
				this.lineageAdds.add(index++, crossResult);
			}
			
			if(operator.getOperatorType() == OperatorType.UNION){
				JUCCondition condition = operator.getJUCCondition();
				DataSet firstInput = sources[condition.getFirstInput()];
				DataSet secondInput = sources[condition.getSecondInput()];
				DataSet<?> unionResult = firstInput.union (secondInput);
				operator.setExampleTuples(unionResult);
				unionResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/UNION"+ctr,WriteMode.OVERWRITE);
				this.opTypeToOperator.put("UNION"+ctr, operator);
				dataStream = unionResult;
				this.lineageAdds.add(index++, unionResult);
			}
		}
	}
	
	public void upStreamPass(List<SingleOperator> operatorTree) throws Exception{
		
		for(int ctr = operatorTree.size();ctr > 0; ctr-- ){
			SingleOperator operator = operatorTree.get(ctr - 1);
			if(operator.getEquivalenceClasses() !=  null)
			for(EquivalenceClass eqClass : operator.getEquivalenceClasses()){
				if(eqClass.hasExample()){
					DataSet constraintRecord = createConstraintRecords(operator);
					//System.out.println(constraintRecord);
				}
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
	
	public Map readDownstreamExamplesIntoCollection(String outputPath)
			throws IOException {

		Map<String, Collection> lineageMap = new HashMap<String, Collection>();
		File outputDirectory = new File(outputPath);
		String line;

		for (File fileEntry : outputDirectory.listFiles()) {
			if (fileEntry.isDirectory()) {
				List exampleSet = new ArrayList();
				for (File insideFile : fileEntry.listFiles()) {
					BufferedReader br = new BufferedReader(new FileReader(insideFile));
					while ((line = br.readLine()) != null) {
						exampleSet.add(line);
					}
					lineageMap.put(fileEntry.getName(), exampleSet);
					br.close();
				}

			}
			else if (fileEntry.isFile()){
				List exampleSet = new ArrayList();
				BufferedReader br = new BufferedReader(new FileReader(fileEntry));
				while ((line = br.readLine()) != null) {
					exampleSet.add(line);
				}
				lineageMap.put(fileEntry.getName(), exampleSet);
				br.close();
			}
		}
		return lineageMap;
	}
	
	public void setEquivalenceClasses(Map lineageMap){
		
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
				
				SingleOperator load = this.opTypeToOperator.get(opName);
				List<EquivalenceClass> eqClass = new ArrayList<EquivalenceClass>();
				eqClass.add(loadEqClasses.getLoadExample());
				load.setEquivalenceClasses(eqClass);
				System.out.println(checkEqclasses(load));
			}
			
			if(opName.contains("JOIN")){
				List joinExamples = (List)lineageMap.get(opName);
				JoinEquivalenceClasses joinEqClasses = new JoinEquivalenceClasses();
				if(!joinExamples.isEmpty()){
					joinEqClasses.getJoinedExample().setHasExample(true);
				}
				else
					joinEqClasses.getJoinedExample().setHasExample(false);
				
				SingleOperator join = this.opTypeToOperator.get(opName);
				List<EquivalenceClass> eqClass = new ArrayList<EquivalenceClass>();
				eqClass.add(joinEqClasses.getJoinedExample());
				join.setEquivalenceClasses(eqClass);
				System.out.println(checkEqclasses(join));
			}
			
		}
	
		
	}	
	
	public void getRecordLineage(Map lineageMap){
		
		Pattern integerOnly = Pattern.compile("\\d+");
		List<Integer> opOrder = new ArrayList<Integer>();
		List<List> lineages = new ArrayList<List>();
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
						List lineage = new ArrayList();
						String lineageLast = (String) exIt.next();
						lineage.add(lineageLast);
						lineages.add(lineage);
						System.out.println(constructRecordLineage(lineageMap, opOrder, id, lineage, lineageLast));
					}
							
				}
			}
		}
		setEquivalenceClasses(lineageMap);
		
	}
	
	public List constructRecordLineage(Map lineageMap, List<Integer> opOrder,
			int id, List lineageGroup, String lineageLast) {

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
							if (nextLineage.contains(lineageLast)) {
								lineageGroup.add(nextLineage);
							}
						}
					}
				}
			}
		}
		return lineageGroup;
	}
	
	public Map checkEqclasses(SingleOperator op){
		Map<String,Boolean> eqClassMap = new HashMap<String, Boolean>();
		for(EquivalenceClass eqClass : op.getEquivalenceClasses()){
			eqClassMap.put(eqClass.getName(), eqClass.hasExample());
		}
		return eqClassMap;
	}
	
	public DataSet createConstraintRecords(SingleOperator operator){
		DataSet dataSetToReturn = new DataSet(this.env, operator.getOperatorOutputType()){};
		TypeInformation outputType = operator.getOperatorOutputType();
		
		System.out.println(outputType);
		
		return dataSetToReturn;
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
