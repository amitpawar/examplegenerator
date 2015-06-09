package thesis.input.operatortree;

import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.tuple.Tuple;
import thesis.algorithm.semantics.EquivalenceClass;

public class SingleOperator {

    private String name;
    private OperatorType operatorType;
    private TypeInformation<?> operatorOutputType;
    private JUCCondition jucCondition;
    private Operator<?> operator;
    private List<EquivalenceClass> equivalenceClasses;
    private List<SingleOperator> parentOperators;
    private List operatorOutputAsList;
    private Tuple constraintRecords;





    public Tuple getConstraintRecords() {
        return constraintRecords;
    }

    public void setConstraintRecords(Tuple constraintRecords) {
        this.constraintRecords = constraintRecords;
    }

    public List getOperatorOutputAsList() {
        return operatorOutputAsList;
    }

    public void setOperatorOutputAsList(List operatorOutputAsList) {
        this.operatorOutputAsList = operatorOutputAsList;
    }


    public List<SingleOperator> getParentOperators() {
        return parentOperators;
    }

    public void setParentOperators(List<SingleOperator> parentOperators) {
        this.parentOperators = parentOperators;
    }


    public List<EquivalenceClass> getEquivalenceClasses() {
        return equivalenceClasses;
    }

    public void setEquivalenceClasses(List<EquivalenceClass> equivalenceClasses) {
        this.equivalenceClasses = equivalenceClasses;
    }

    public Operator<?> getOperator() {
        return operator;
    }

    public void setOperator(Operator<?> operator) {
        this.operator = operator;
    }

   public JUCCondition getJUCCondition() {
        return jucCondition;
    }

    public void setJUCCondition(JUCCondition joinCondition) {
        this.jucCondition = joinCondition;
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


    public OperatorType getOperatorType(){
        return this.operatorType;
    }

    public void setOperatorType(OperatorType type){
        this.operatorType = type;
    }



    public class JUCCondition {
        private OperatorType operatorType;
        private int firstInput;
        private int secondInput;
        private int[] firstInputKeyColumns;
        private int[] secondInputKeyColumns;

        public OperatorType getOperatorType() {
            return operatorType;
        }

        public void setOperatorType(OperatorType operatorType) {
            this.operatorType = operatorType;
        }
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
        public int getSecondInput() {
            return secondInput;
        }
        public void setSecondInput(int secondInput) {
            this.secondInput = secondInput;
        }
    }

}


