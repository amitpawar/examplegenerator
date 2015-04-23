package thesis.input.operatortree;

import java.util.List;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class SingleOperator {
	
	private String name;
	private OperatorType operatorType;
	private String previousOperator;
	private OperatorType previousOperatorType;
	private TypeInformation<?> operatorOutputType;
	private List<TypeInformation<?>> operatorInputType;
	private JoinCondition joinCondition;
	private Operator<?> operator;
	
	

	public Operator<?> getOperator() {
		return operator;
	}

	public void setOperator(Operator<?> operator) {
		this.operator = operator;
	}

	public JoinCondition getJoinCondition() {
		return joinCondition;
	}

	public void setJoinCondition(JoinCondition joinCondition) {
		this.joinCondition = joinCondition;
	}

	public List<TypeInformation<?>> getOperatorInputType() {
		return operatorInputType;
	}

	public void setOperatorInputType(List<TypeInformation<?>> operatorInputType) {
		this.operatorInputType = operatorInputType;
	}

	public TypeInformation<?> getOperatorOutputType() {
		return operatorOutputType;
	}

	public void setOperatorOutputType(TypeInformation<?> operatorOutputType) {
		this.operatorOutputType = operatorOutputType;
	}

	public String getOperatorName(){
		return this.name;
	}
	
	public void setOperatorName(String name){
		this.name = name;
	}
	
	public String getPreviousOperatorName(){
		return this.previousOperator;
	}
	
	public void setPreviousOperatorName(String name){
		this.previousOperator = name;
	}
	
	public OperatorType getOperatorType(){
		return this.operatorType;
	}
	
	public void setOperatorType(OperatorType type){
		this.operatorType = type;
	}
	
	public OperatorType getPreviousOperatorType(){
		return this.previousOperatorType;
	}
	
	public void setPreviousOperatorType(OperatorType type){
		this.previousOperatorType = type;
	}
	
	
	public class JoinCondition {
		private int firstInput;
		private int secontInput;
		private int[] firstInputKeyColumns;
		private int[] secondInputKeyColumns;
	
		public int[] getFirstInputKeyColumns() {
			return firstInputKeyColumns;
		}
		public void setFirstInputKeyColumns(int[] firstInputKeyColumns) {
			this.firstInputKeyColumns = firstInputKeyColumns;
		}
		public int[] getSecondInputKeyColumns() {
			return secondInputKeyColumns;
		}
		public void setSecondInputKeyColumns(int[] secondInputKeyColumns) {
			this.secondInputKeyColumns = secondInputKeyColumns;
		}
	
		public int getFirstInput() {
			return firstInput;
		}
		public void setFirstInput(int firstInput) {
			this.firstInput = firstInput;
		}
		public int getSecontInput() {
			return secontInput;
		}
		public void setSecontInput(int secontInput) {
			this.secontInput = secontInput;
		}
	
		
	}

}


