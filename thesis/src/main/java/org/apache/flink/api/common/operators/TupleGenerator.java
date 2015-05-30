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
    private Map<String, SingleOperator> opTypeShortNameToOperator = new HashMap<String, SingleOperator>();
    private ExecutionEnvironment env;
    private List<DataSet> lineageAdds = new ArrayList<DataSet>();
    private Map<String, DataSet> unUsedExamplesToSourceMap = new HashMap<String, DataSet>();
    private final String downstreamOutputPath = "/home/amit/thesis/output3/TEST/downStream";
    private Map<SingleOperator, Tuple> operatorToConstraintRecordMap = new HashMap<SingleOperator, Tuple>();
    private Object joinKey = null;
    private int maxRecords = -1;

    public List<DataSet> getLineageAdds() {
        return lineageAdds;
    }

    public void setLineageAdds(List<DataSet> lineageAdds) {
        this.lineageAdds = lineageAdds;
    }

    public TupleGenerator(List<InputDataSource> dataSources,
                          List<SingleOperator> operatorTree, ExecutionEnvironment env, int maxRecords) throws Exception {
        this.dataSources = dataSources;
        this.operatorTree = operatorTree;
        this.env = env;
        this.maxRecords = maxRecords;
        downStreamPass(this.operatorTree);

        setEquivalenceClasses();
        upStreamPass(this.operatorTree);
        afterUpstreampass(this.operatorTree);
        //System.out.println(this.lineageGroup);
    }


    public void downStreamPass(List<SingleOperator> operatorTree) throws Exception {
        for (int i = 0; i < operatorTree.size(); i++) {
            SingleOperator operator = operatorTree.get(i);

            if (operator.getOperatorType() == OperatorType.SOURCE) {

                List list = ((GenericDataSourceBase) operator.getOperator()).executeOnCollections(this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("SOURCE " + operator.getOperatorName());
                for (Object object : list)
                    System.out.println(object);
            }


            if (operator.getOperatorType() == OperatorType.LOAD) {

                List inputList = new ArrayList();
                Random randomGenerator = new Random();
                for(int j = 0; j < this.maxRecords; j++) {
                    inputList.add(returnRandomTuple(operator.getParentOperators().get(0).getOperatorOutputAsList(), randomGenerator));
                }


                List list = ((SingleInputOperator) operator.getOperator()).executeOnCollections(inputList,
                        null, this.env.getConfig());
                operator.setOperatorOutputAsList(list);
                System.out.println("LOAD " + operator.getOperatorName());
                for (Object object : list)
                    System.out.println(object);
            }

            if(operator.getOperatorType() != OperatorType.SOURCE && operator.getOperatorType() != OperatorType.LOAD){
                List output = executeIndividualOperator(operator);
                System.out.println(operator.getOperatorType());
                operator.setOperatorOutputAsList(output);
                for (Object object : output)
                    System.out.println(object);

            }
        }
    }
    public void afterUpstreampass(List<SingleOperator> operatorTree) throws Exception {
        for (int i = 0; i < operatorTree.size(); i++) {
            SingleOperator operator = operatorTree.get(i);

            if(operator.getOperatorType() != OperatorType.SOURCE && operator.getOperatorType() != OperatorType.LOAD){
                List output = executeIndividualOperator(operator);
                System.out.println("After upstream------------"+operator.getOperatorType());
                operator.setOperatorOutputAsList(output);
                for (Object object : output)
                    System.out.println(object);

            }
        }
    }

    public List executeIndividualOperator(SingleOperator singleOperator) throws Exception {
        List output = new ArrayList();
        Operator operator = singleOperator.getOperator();
        if(operator instanceof SingleInputOperator){
            List input1 = singleOperator.getParentOperators().get(0).getOperatorOutputAsList();
            output = ((SingleInputOperator) operator).executeOnCollections(input1,null,this.env.getConfig());
        }
        if(operator instanceof DualInputOperator){
            List input1 = singleOperator.getParentOperators().get(0).getOperatorOutputAsList();
            List input2 = singleOperator.getParentOperators().get(1).getOperatorOutputAsList();
            output = ((DualInputOperator) operator).executeOnCollections(input1,input2,null,this.env.getConfig());
        }
        return output;
    }

    public Object returnRandomTuple(List parentOutput, Random randomGenerator) {
        int index = randomGenerator.nextInt(parentOutput.size());
        return parentOutput.get(index);
    }

    public void upStreamPass(List<SingleOperator> operatorTree) throws Exception {

        for (int ctr = operatorTree.size(); ctr > 0; ctr--) {
            SingleOperator operator = operatorTree.get(ctr - 1);
            //todo logic for not including source
            if (operator.getEquivalenceClasses() != null && operator.getOperatorType() == OperatorType.JOIN) {
                for (EquivalenceClass eqClass : operator.getEquivalenceClasses()) {
                    if (true) {
                        //if (!eqClass.hasExample()) {
                        JUCCondition joinCondition = operator.getJUCCondition();

                        String[] firstTokens = constructJoinConstraintTokens(joinCondition, operator.getParentOperators().get(0).getOperatorOutputType().getTotalFields(), 0);
                        Tuple parent1Tuple = getConstraintRecord(operator.getParentOperators().get(0),
                                new LinkedList<String>(Arrays.asList(firstTokens)));
                        operator.getParentOperators().get(0).getOperatorOutputAsList().add(parent1Tuple);
                        operator.getParentOperators().get(0).setConstraintRecords(parent1Tuple);
                        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(0), parent1Tuple);
                        propagateConstraintRecordUpstream(operator.getParentOperators().get(0), parent1Tuple, operator);

                        String[] secondTokens = constructJoinConstraintTokens(joinCondition, operator.getParentOperators().get(1).getOperatorOutputType().getTotalFields(), 1);
                        Tuple parent2Tuple = getConstraintRecord(operator.getParentOperators().get(1),
                                new LinkedList<String>(Arrays.asList(secondTokens)));
                        operator.getParentOperators().get(1).getOperatorOutputAsList().add(parent2Tuple);
                        operator.getParentOperators().get(1).setConstraintRecords(parent2Tuple);
                        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(1), parent2Tuple);
                        propagateConstraintRecordUpstream(operator.getParentOperators().get(1), parent2Tuple, operator);

                    }
                }

            }

        }
    }

    public String[] constructJoinConstraintTokens(JUCCondition joinCondition, int totalFields, int inputNum) throws IllegalAccessException, InstantiationException {

        int keyColumn = (inputNum == 0) ? joinCondition.getFirstInputKeyColumns()[0] : joinCondition.getSecondInputKeyColumns()[0];
        String[] tokens = new String[totalFields];
        for (int i = 0; i < totalFields; i++) {
            if (i == keyColumn) {
                tokens[i] = "JOINKEY";
            } else
                tokens[i] = "DONTCARE";
        }
        return tokens;
    }

    public void convertConstraintRecordToConcreteRecord(SingleOperator child, SingleOperator operatorWithEmptyEqClass) throws Exception {
        //child = leaf , parent = basetable
        Map<SingleOperator, List> loadOperatorWithUnUsedExamples = new LinkedHashMap<SingleOperator, List>();
        SingleOperator parent = child.getParentOperators().get(0);
        //convert only if its a leaf operator
        if (parent.getOperator() instanceof GenericDataSourceBase) {
            System.out.println("UNUSED-----");
            List unUsedExamplesAtLeaf = getUnusedExamplesFromBaseTable(parent, child, child.getOperatorOutputAsList());
            loadOperatorWithUnUsedExamples.put(child, unUsedExamplesAtLeaf);
            Tuple constraintRecord = child.getConstraintRecords();
            for (int i = 0; i < constraintRecord.getArity(); i++) {
                if (constraintRecord.getField(i) == "JOINKEY") {
                    Random random = new Random();
                    //todo in case of multiple joins
                    if (this.joinKey == null)
                        this.joinKey = ((Tuple) returnRandomTuple(unUsedExamplesAtLeaf, random)).getField(i);
                    constraintRecord.setField(this.joinKey, i);
                }
                if (constraintRecord.getField(i) == "DONTCARE") {
                    Random random = new Random();
                    Object randomValue = ((Tuple) returnRandomTuple(unUsedExamplesAtLeaf, random)).getField(i);
                    constraintRecord.setField(randomValue, i);

                }

            }
            System.out.println("-------------------Changed Constraint Record-----------" + constraintRecord);
           // child.getOperatorOutputAsList().add(constraintRecord);
        }
    }


    public List getUnusedExamplesFromBaseTable(SingleOperator baseOperator, SingleOperator leafOperator, List usedExamples) throws Exception {

        List allExamples = baseOperator.getOperatorOutputAsList();
        List allExamplesAtLeaf = ((SingleInputOperator) leafOperator.getOperator())
                .executeOnCollections(allExamples, null, this.env.getConfig());

        allExamplesAtLeaf.removeAll(usedExamples);

        return allExamplesAtLeaf;

    }

    public void propagateConstraintRecordUpstream(SingleOperator childOperator, Tuple constraintRecord, SingleOperator operatorWithEmptyEqClass) throws Exception {
        for (SingleOperator parent : childOperator.getParentOperators()) {
            if (childOperator.getOperatorType() != OperatorType.LOAD) {
                while (parent.getOperatorType() != OperatorType.LOAD) {
                    parent = parent.getParentOperators().get(0);
                    parent.getOperatorOutputAsList().add(constraintRecord);
                    parent.setConstraintRecords(constraintRecord);
                    this.operatorToConstraintRecordMap.put(parent, constraintRecord);
                }
                parent.getOperatorOutputAsList().add(constraintRecord);
                parent.setConstraintRecords(constraintRecord);
                this.operatorToConstraintRecordMap.put(parent, constraintRecord);
                //parent is LOAD, once load is reached change to concrete
                convertConstraintRecordToConcreteRecord(parent, operatorWithEmptyEqClass);
            }
        }
    }

    public void pruneTuples() {

    }


    public Map readExampeTuples() {
        Map<String, Collection> lineageMap = new HashMap<String, Collection>();
        for (SingleOperator operator : this.operatorTree) {
            lineageMap.put(operator.getOperatorName(), operator.getOperatorOutputAsList());
        }
        return lineageMap;
    }

    public void setEquivalenceClasses() {
        for (SingleOperator operator : this.operatorTree) {

            if (operator.getOperatorType() == OperatorType.LOAD) {
                List loadExamples = operator.getOperatorOutputAsList();
                LoadEquivalenceClasses loadEquivalenceClass = new LoadEquivalenceClasses();

                if (!loadExamples.isEmpty()) {
                    loadEquivalenceClass.getLoadExample().setHasExample(true);
                } else
                    loadEquivalenceClass.getLoadExample().setHasExample(false);

                List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
                equivalenceClasses.add(loadEquivalenceClass.getLoadExample());
                operator.setEquivalenceClasses(equivalenceClasses);
            }

            if (operator.getOperatorType() == OperatorType.JOIN) {
                List joinExamples = operator.getOperatorOutputAsList();
                JoinEquivalenceClasses joinEquivalenceClass = new JoinEquivalenceClasses();
                if (!joinExamples.isEmpty()) {
                    joinEquivalenceClass.getJoinedExample().setHasExample(true);
                } else
                    joinEquivalenceClass.getJoinedExample().setHasExample(false);


                List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
                equivalenceClasses.add(joinEquivalenceClass.getJoinedExample());
                operator.setEquivalenceClasses(equivalenceClasses);

            }
        }
    }


    public String checkEquivalenceClasses(SingleOperator operator) {
        String returnString = "";
        if (operator.getEquivalenceClasses() != null) {
            for (EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()) {
                if (equivalenceClass.hasExample())
                    returnString = returnString + " " + equivalenceClass.getName() + " " + equivalenceClass.hasExample();
                else
                    returnString = returnString + " " + equivalenceClass.getName() + " false";
            }
        } else
            returnString = returnString + " " + operator.getOperatorName() + " eq not set";
        return returnString;
    }


    public Tuple getConstraintRecord(SingleOperator operator, List tokens) throws IllegalAccessException, InstantiationException {

        Tuple constraintRecord = drillToBasicType(operator.getOperatorOutputType(), tokens);
        return constraintRecord;
    }


    //Todo : check for all types
    public Tuple drillToBasicType(TypeInformation typeInformation, List tokens) throws IllegalAccessException, InstantiationException {
        Tuple testTuple = (Tuple) typeInformation.getTypeClass().newInstance();

        for (int ctr = 0; ctr < typeInformation.getArity(); ctr++) {

            if (((CompositeType) typeInformation).getTypeAt(ctr).isTupleType())
                testTuple.setField(drillToBasicType(((CompositeType) typeInformation).getTypeAt(ctr), tokens), ctr);

            else {
                String name = ((CompositeType) typeInformation).getTypeAt(ctr).toString();//todo add tuple field from source

                testTuple.setField(tokens.get(0), ctr);
                tokens.remove(tokens.get(0));
                /*if (name.equalsIgnoreCase("String"))
                    testTuple.setField(name, ctr);
                if (name.equalsIgnoreCase("Long")) {
                    long someValue = -9999;
                    testTuple.setField(someValue, ctr);

                }*/
            }
        }
        return testTuple;


    }


    /////////////////////////////

    public static class TupleFilter extends RichFilterFunction {

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
