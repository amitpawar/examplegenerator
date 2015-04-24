package thesis.algorithm.logic;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import thesis.input.operatortree.SingleOperator;
import thesis.input.operatortree.OperatorType;

public class TupleGenerator {
	
	public void generateTuples(ExecutionEnvironment env, List<DataSet<?>> dataSources, List<SingleOperator> operatorTree) throws Exception{
		
		for(SingleOperator operator : operatorTree){
			if(operator.getOperatorType() == OperatorType.LOAD){
				dataSources.get(1).getType();
				DataSet<Tuple2<?,?>> dataset = (DataSet<Tuple2<?, ?>>) dataSources.get(1).first(2);
				Set<Tuple2<?,?>> coll = new HashSet<Tuple2<?,?>>();
				
				RemoteCollectorImpl.collectLocal(dataset, coll);
				
				System.out.println("Set "+coll);
				for(Tuple2<?, ?>tups : coll ){
					coll.remove(tups);
				}
				Tuple2<?, ?> addtup = new Tuple2<String, String>("Test", "Test");
				coll.add(addtup);
				DataSet<Tuple2<?,?>> newDS = env.fromCollection(coll);
				newDS.print();
				env.execute();
				
				
				if(dataSources.get(1).getType().getTypeClass().newInstance() instanceof  Tuple2){
					//System.out.println(dataSources.get(1).getType().getTypeClass().newInstance());
				}
				
				
				switch (dataSources.get(1).getType().getTotalFields()){
					
					case 1: DataSet<Tuple1<?>> dataSet1 = (DataSet<Tuple1<?>>) dataSources.get(1);
							Set<Tuple1<?>> coll1 = new HashSet<Tuple1<?>>();
							RemoteCollectorImpl.collectLocal(dataSet1, coll1);
							DataSet<Tuple1<?>> fromColl = env.fromCollection(coll1);
				}
			}
		}
		
		
	}
	
	

}
