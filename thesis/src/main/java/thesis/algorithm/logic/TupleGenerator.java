package thesis.algorithm.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import scala.Array;
import thesis.examples.Config;
import thesis.input.datasources.InputDataSource;
import thesis.input.operatortree.SingleOperator;
import thesis.input.operatortree.OperatorType;
import thesis.input.operatortree.SingleOperator.JUCCondition;

public class TupleGenerator {

	private List<InputDataSource> dataSources;
	private List<SingleOperator> operatorTree;

	public TupleGenerator(List<InputDataSource> dataSources,
			List<SingleOperator> operatorTree) throws Exception {
		this.dataSources = dataSources;
		this.operatorTree = operatorTree;
		//generateTuples(this.dataSources, this.operatorTree);
		downStreamPass(this.dataSources, this.operatorTree);
	}

	@SuppressWarnings("unchecked")
	public void generateTuplesTest(ExecutionEnvironment env,
			List<DataSet<?>> dataSources, List<SingleOperator> operatorTree)
			throws Exception {

		for (SingleOperator operator : operatorTree) {
			if (operator.getOperatorType() == OperatorType.LOAD) {
				dataSources.get(1).getType();
				DataSet<Tuple2<?, ?>> dataset = (DataSet<Tuple2<?, ?>>) dataSources.get(1).first(2);

				Set<Tuple2<?, ?>> coll = new HashSet<Tuple2<?, ?>>();

				RemoteCollectorImpl.collectLocal(dataset, coll);
				// env.execute();

				Tuple2<?, ?> addtup = new Tuple2<String, String>("Test", "Test");
				coll.add(addtup);
				DataSet<Tuple2<?, ?>> newDS = env.fromCollection(coll);
				newDS.print();
				env.execute();
				System.out.println("Set " + coll);
				RemoteCollectorImpl.shutdownAll();

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

	public void downStreamPass(List<InputDataSource> dataSources,List<SingleOperator> operatorTree){
		
		DataSet<?> dataStream = null ;
		DataSet<?>[] sources = new DataSet<?>[dataSources.size()];
		for (int i = 0; i < dataSources.size(); i++){
			sources[i] = dataSources.get(i).getDataSet();
			//sources[i].writeAsCsv(Config.outputPath()+"/TEST/downStream/LOAD"+i,WriteMode.OVERWRITE);
		}
		
		for(SingleOperator operator : operatorTree){
			if(operator.getOperatorType() == OperatorType.LOAD){
				int id = operator.getOperatorInputDataSetId().get(0);
				sources[id] = sources[id].first(2);
				sources[id].writeAsCsv(Config.outputPath()+"/TEST/downStream/LOAD"+id,WriteMode.OVERWRITE);
			}
			
			if(operator.getOperatorType() == OperatorType.JOIN){
				int ctr = 0;
				JUCCondition condition = operator.getJUCCondition();
				DataSet<?> joinResult = 
						sources[condition.getFirstInput()].join(sources[condition.getSecondInput()])
						.where(condition.getFirstInputKeyColumns())
						.equalTo(condition.getSecondInputKeyColumns());
				joinResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/JOIN"+ctr++,WriteMode.OVERWRITE);
				dataStream = joinResult;
			}
			
			if(operator.getOperatorType() == OperatorType.PROJECT){
				int ctr = 0;
				DataSet<?> projResult = dataStream.project(operator.getProjectColumns());
				projResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/PROJECT"+ctr++,WriteMode.OVERWRITE);
				dataStream = projResult;
			}
			
			if(operator.getOperatorType() == OperatorType.CROSS){
				int ctr = 0;
				JUCCondition condition = operator.getJUCCondition();
				DataSet<?> crossResult = 
						sources[condition.getFirstInput()].cross(sources[condition.getSecondInput()]);
				crossResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/CROSS"+ctr++,WriteMode.OVERWRITE);
				dataStream = crossResult;
			}
			
			if(operator.getOperatorType() == OperatorType.UNION){
				int ctr = 0;
				JUCCondition condition = operator.getJUCCondition();
				DataSet firstInput = sources[condition.getFirstInput()];
				DataSet secondInput = sources[condition.getSecondInput()];
				DataSet<?> unionResult = firstInput.union (secondInput);
						//sources[condition.getFirstInput()].union(sources[condition.getSecondInput()]);
				unionResult.writeAsCsv(Config.outputPath()+"/TEST/downStream/UNION"+ctr++,WriteMode.OVERWRITE);
				dataStream = unionResult;
			}
		}
	}
	
	public void upStreamPass(){
		
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
