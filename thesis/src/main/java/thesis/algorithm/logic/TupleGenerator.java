package thesis.algorithm.logic;

import java.util.List;

import org.apache.flink.api.java.DataSet;

import thesis.input.operatortree.SingleOperator;
import thesis.input.operatortree.OperatorType;

public class TupleGenerator {
	
	public void generateTuples(List<DataSet<?>> dataSources, List<SingleOperator> operatorTree){
		
		for(SingleOperator operator : operatorTree){
			if(operator.getOperatorType() == OperatorType.LOAD){
				dataSources.get(0).first(2).print();
			}
		}
	}

}
