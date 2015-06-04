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
    private String previousOperator;
    private OperatorType previousOperatorType;
    private TypeInformation<?> operatorOutputType;
    private List<TypeInformation<?>> operatorInputType;
    private JUCCondition jucCondition;
    private Operator<?> operator;
    private List<Integer> operatorInputDataSetId;
    private List<DataSet<?>> operatorDataSets;
    private DataSet<?> outputExampleTuples;
    private int[] projectColumns;
    private List<EquivalenceClass> equivalenceClasses;
    private List<SingleOperator> parentOperators;
    private int outputDataSetId;
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


    public int getOutputDataSetId() {
        return outputDataSetId;
    }

    public void setOutputDataSetId(int outputDataSetId) {
        this.outputDataSetId = outputDataSetId;
    }

    public List<Integer> getOperatorInputDataSetId() {
        return operatorInputDataSetId;
    }

    public void setOperatorInputDataSetId(List<Integer> operatorInputDataSetId) {
        this.operatorInputDataSetId = operatorInputDataSetId;
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

    public int[] getProjectColumns() {
        return projectColumns;
    }

    public void setProjectColumns(int[] projectColumns) {
        this.projectColumns = projectColumns;
    }

    public DataSet<?> getOutputExampleTuples() {
        return outputExampleTuples;
    }

    public void setOutputExampleTuples(DataSet<?> outputExampleTuples) {
        this.outputExampleTuples = outputExampleTuples;
    }

    public List<DataSet<?>> getOperatorDataSets() {
        return operatorDataSets;
    }

    public void setOperatorDataSets(List<DataSet<?>> operatorDataSets) {
        this.operatorDataSets = operatorDataSets;
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


