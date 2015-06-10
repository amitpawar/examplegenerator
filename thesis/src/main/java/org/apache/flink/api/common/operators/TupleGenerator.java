package org.apache.flink.api.common.operators;

import java.util.*;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import thesis.algorithm.semantics.*;
import thesis.input.operatortree.SingleOperator;
import thesis.input.operatortree.OperatorType;
import thesis.input.operatortree.SingleOperator.JUCCondition;

public class TupleGenerator {

    private List<SingleOperator> operatorTree;
    private ExecutionEnvironment env;
    private Map<SingleOperator, Tuple> operatorToConstraintRecordMap = new HashMap<SingleOperator, Tuple>();
    private Object joinKey = null;
    private int maxRecords = -1;
    private Map<Object, LinkedHashMap<SingleOperator, Object>> lineageTracker = new HashMap<Object, LinkedHashMap<SingleOperator, Object>>();
    private static String dontCareString = "DONOTCAREWHATGOESHERE";
    private static Integer dontCareInteger = -12345;
    private static Long dontCareLong = Long.valueOf(-123456789);
    private static Double dontCareDouble = -1.2345;
    private static String joinKeyString = "JOINKEYISHERE";
    private static Integer joinKeyInteger = -9999;
    private static Long joinKeyLong = Long.valueOf(-99999999);
    private static Double joinKeyDouble = -9.9999;


    public TupleGenerator(List<SingleOperator> operatorTree, ExecutionEnvironment env, int maxRecords) throws Exception {
        this.operatorTree = operatorTree;
        this.env = env;
        this.maxRecords = maxRecords;
        downStreamPass(this.operatorTree);
        setEquivalenceClasses();
        System.out.println("After Downstream" + Strings.repeat("-", 200));
        displayExamples(this.operatorTree);
        upStreamPass(this.operatorTree);
        afterUpstreampass(this.operatorTree);
        setEquivalenceClasses();
        System.out.println("After Upstream" + Strings.repeat("-", 200));
        displayExamples(this.operatorTree);
        pruneTuples();
        System.out.println("After Pruning" + Strings.repeat("-", 200));
        displayExamples(this.operatorTree);

    }

    public void downStreamPass(List<SingleOperator> operatorTree) throws Exception {
        for (int i = 0; i < operatorTree.size(); i++) {

            SingleOperator operator = operatorTree.get(i);

            if (operator.getOperatorType() == OperatorType.SOURCE) {

                List list = ((GenericDataSourceBase) operator.getOperator()).executeOnCollections(this.env.getConfig());
                operator.setOperatorOutputAsList(list);

            }
        }
        for (int j = 0; j < this.maxRecords; j++) {
            for (int k = 0; k < operatorTree.size(); k++) {
                SingleOperator operator = operatorTree.get(k);
                if (operator.getOperatorType() != OperatorType.SOURCE) {
                    executeOperatorPerRecord(operator);
                }
            }
        }
        //displayExamples(operatorTree);

    }

    public void executeOperatorPerRecord(SingleOperator operator) throws Exception {

        if (operator.getOperatorType() != OperatorType.SOURCE) {
            if (operator.getOperatorType() == OperatorType.LOAD) {

                List inputList = new ArrayList();
                Random randomGenerator = new Random();
                if (!operator.getParentOperators().get(0).getOperatorOutputAsList().isEmpty())
                    inputList.add(returnRandomTuple(operator.getParentOperators().get(0).getOperatorOutputAsList(), randomGenerator));
                List output = ((SingleInputOperator) operator.getOperator()).executeOnCollections(inputList, null, this.env.getConfig());


                if (operator.getOperatorOutputAsList() != null)
                    operator.getOperatorOutputAsList().addAll(output);
                else
                    operator.setOperatorOutputAsList(output);

                if (!output.isEmpty())
                    addToLineageTracer(output.get(0), operator, output.get(0));

            } else {
                List output = executeIndividualOperator(operator);


                if (operator.getOperatorOutputAsList() != null) {
                    operator.getOperatorOutputAsList().clear();
                    operator.getOperatorOutputAsList().addAll(output);
                } else
                    operator.setOperatorOutputAsList(output);

            }

        }
    }

    public void addToLineageTracer(Object inputExample, SingleOperator operator, Object outputExample) {

        if (!checkIfAlreadyInTracer(inputExample, outputExample, operator)) {
            if (this.lineageTracker.containsKey(inputExample)) {
                Map<SingleOperator, Object> exampleTracker = this.lineageTracker.get(inputExample);
                exampleTracker.put(operator, outputExample);
            } else {
                LinkedHashMap<SingleOperator, Object> exampleTracker = new LinkedHashMap<SingleOperator, Object>();
                exampleTracker.put(operator, outputExample);
                this.lineageTracker.put(inputExample, exampleTracker);
            }
        }
    }

    public boolean checkIfAlreadyInTracer(Object inputExample, Object outputExample, SingleOperator operator) {
        boolean flag = false;
        for (Map<SingleOperator, Object> recordTracer : this.lineageTracker.values()) {
            if (recordTracer.values().contains(inputExample)) {
                recordTracer.put(operator, outputExample);
                flag = true;
            }
        }
        return flag;
    }

    public void displayExamples(List<SingleOperator> operatorTree) {
        for (SingleOperator operator : operatorTree) {
            if (operator.getOperatorType() != OperatorType.SOURCE) {
                System.out.println(operator.getOperatorType() + " " + operator.getOperatorName());
                for (Object object : operator.getOperatorOutputAsList())
                    System.out.println(object);
            }
        }
    }

    public void afterUpstreampass(List<SingleOperator> operatorTree) throws Exception {
        for (int i = 0; i < operatorTree.size(); i++) {
            SingleOperator operator = operatorTree.get(i);

            if (operator.getOperatorType() != OperatorType.SOURCE && operator.getOperatorType() != OperatorType.LOAD) {
                List output = executeIndividualOperator(operator);
                // System.out.println("After upstream------------" + operator.getOperatorType());
                operator.setOperatorOutputAsList(output);
                // for (Object object : output)
                // System.out.println(object);

            }
        }

    }

    public List executeIndividualOperator(SingleOperator singleOperator) throws Exception {
        List<Object> output = new ArrayList();
        Operator operator = singleOperator.getOperator();
        if (operator instanceof SingleInputOperator) {

            List<Object> input1 = singleOperator.getParentOperators().get(0).getOperatorOutputAsList();

            for (List<Object> singleExample : Lists.partition(input1, 1)) {

                List outputExample = ((SingleInputOperator) operator).executeOnCollections(singleExample, null, this.env.getConfig());
                if (!outputExample.isEmpty()) {
                    if (singleOperator.getOperatorType() == OperatorType.DISTINCT && !output.contains(outputExample.get(0)))
                        output.add(outputExample.get(0));
                    if (singleOperator.getOperatorType() != OperatorType.DISTINCT)
                        output.add(outputExample.get(0));

                    addToLineageTracer(singleExample.get(0), singleOperator, outputExample.get(0));
                }
            }
            //output = ((SingleInputOperator) operator).executeOnCollections(input1, null, this.env.getConfig());
        }
        if (operator instanceof DualInputOperator) {
            List<Object> input1 = singleOperator.getParentOperators().get(0).getOperatorOutputAsList();
            List<Object> input2 = singleOperator.getParentOperators().get(1).getOperatorOutputAsList();


            for (List<Object> singleExample : Lists.partition(input1, 1)) {
                List outputExamples = ((DualInputOperator) operator).executeOnCollections(singleExample, input2, null, this.env.getConfig());
                if (!outputExamples.isEmpty()) {
                    for (Object outputExample : outputExamples) {
                        if (!output.contains(outputExample)) {
                            output.add(outputExample);
                        }
                        if (singleOperator.getOperatorType() != OperatorType.UNION)
                            addToLineageTracer(singleExample.get(0), singleOperator, outputExample);
                        if (singleOperator.getOperatorType() == OperatorType.UNION)
                            addToLineageTracer(singleExample.get(0), singleOperator, singleExample.get(0));
                    }
                }
            }
            for (List<Object> singleExample : Lists.partition(input2, 1)) {
                List outputExamples = ((DualInputOperator) operator).executeOnCollections(input1, singleExample, null, this.env.getConfig());
                if (!outputExamples.isEmpty()) {
                    for (Object outputExample : outputExamples) {
                        if (!output.contains(outputExample)) {
                            output.add(outputExample);
                        }
                        if (singleOperator.getOperatorType() != OperatorType.UNION)
                            addToLineageTracer(singleExample.get(0), singleOperator, outputExample);
                        if (singleOperator.getOperatorType() == OperatorType.UNION)
                            addToLineageTracer(singleExample.get(0), singleOperator, singleExample.get(0));

                    }
                }
            }
            // output = ((DualInputOperator) operator).executeOnCollections(input1, input2, null, this.env.getConfig());
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
            if (operator.getEquivalenceClasses() != null) {
                if (operator.getOperatorType() == OperatorType.JOIN) {
                    for (EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()) {
                        if (!equivalenceClass.hasExample()) {
                            fillJoinEquivalenceClass(operator);
                        }
                    }
                }
                if (operator.getOperatorType() == OperatorType.CROSS || operator.getOperatorType() == OperatorType.UNION) {
                    //cross union will have 2 eq classes, one for each table
                    for (EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()) {
                        if (!equivalenceClass.hasExample()) {
                            //System.out.println(equivalenceClass.getName()+" is empty");
                            fillUnionCrossEquivalenceClass(operator, equivalenceClass);
                        }
                    }
                }

                if(operator.getOperatorType() == OperatorType.FILTER){
                    for(EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()){
                        if(!equivalenceClass.hasExample()){
                           fillFilterEquivalenceClass(operator,equivalenceClass);
                        }
                    }
                }
            }
        }
    }

    public void fillFilterEquivalenceClass(SingleOperator operator, EquivalenceClass equivalenceClass) throws Exception {
        String[] tokens = constructUnionCrossConstraintTokens(operator.getParentOperators().get(0).getOperatorOutputType());
        Tuple parentTuple = getConstraintRecord(operator.getParentOperators().get(0),
                new LinkedList<String>(Arrays.asList(tokens)));
        operator.getParentOperators().get(0).getOperatorOutputAsList().add(parentTuple);
        operator.getParentOperators().get(0).setConstraintRecords(parentTuple);
        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(0),parentTuple);
        propagateConstraintRecordUpstream(operator.getParentOperators().get(0),parentTuple);
    }

    public void fillUnionCrossEquivalenceClass(SingleOperator operator, EquivalenceClass equivalenceClass) throws Exception {

        int parentId = -1;
        if (equivalenceClass.getName().equalsIgnoreCase("FirstTableExample"))
            parentId = 0;
        if (equivalenceClass.getName().equalsIgnoreCase("SecondTableExample"))
            parentId = 1;
        String[] tokens = constructUnionCrossConstraintTokens(operator.getParentOperators().get(parentId).getOperatorOutputType());
        Tuple parentTuple = getConstraintRecord(operator.getParentOperators().get(parentId),
                new LinkedList<String>(Arrays.asList(tokens)));
        operator.getParentOperators().get(parentId).getOperatorOutputAsList().add(parentTuple);
        operator.getParentOperators().get(parentId).setConstraintRecords(parentTuple);
        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(parentId), parentTuple);
        propagateConstraintRecordUpstream(operator.getParentOperators().get(parentId), parentTuple);
   }

    public void fillJoinEquivalenceClass(SingleOperator operator) throws Exception {
        JUCCondition joinCondition = operator.getJUCCondition();

        //if eqclass is empty, add constraint record to parents such that it fills eqclass
        String[] firstTokens = constructJoinConstraintTokens(joinCondition, operator.getParentOperators().get(0).getOperatorOutputType(), 0);
        Tuple parent1Tuple = getConstraintRecord(operator.getParentOperators().get(0),
                new LinkedList<String>(Arrays.asList(firstTokens)));
        //added the constraint record to parent with junk data (JOINKEY, DONTCARE)
        operator.getParentOperators().get(0).getOperatorOutputAsList().add(parent1Tuple);
        operator.getParentOperators().get(0).setConstraintRecords(parent1Tuple);
        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(0), parent1Tuple);
        //propagate this record till load/leaf
        propagateConstraintRecordUpstream(operator.getParentOperators().get(0), parent1Tuple);

        String[] secondTokens = constructJoinConstraintTokens(joinCondition, operator.getParentOperators().get(1).getOperatorOutputType(), 1);
        Tuple parent2Tuple = getConstraintRecord(operator.getParentOperators().get(1),
                new LinkedList<String>(Arrays.asList(secondTokens)));
        operator.getParentOperators().get(1).getOperatorOutputAsList().add(parent2Tuple);
        operator.getParentOperators().get(1).setConstraintRecords(parent2Tuple);
        this.operatorToConstraintRecordMap.put(operator.getParentOperators().get(1), parent2Tuple);
        propagateConstraintRecordUpstream(operator.getParentOperators().get(1), parent2Tuple);
        this.joinKey = null;
    }

    //todo : more than one key columns ? tuple as a key column ?
    public String[] constructJoinConstraintTokens(JUCCondition joinCondition, TypeInformation typeInformation, int inputNum) throws IllegalAccessException, InstantiationException {

        int totalFields = typeInformation.getTotalFields();
        int arity = typeInformation.getArity();
        int keyColumn = (inputNum == 0) ? joinCondition.getFirstInputKeyColumns()[0] : joinCondition.getSecondInputKeyColumns()[0];
        String[] tokens = new String[totalFields];
        for (int i = 0; i < totalFields; i++) {
            if (i == keyColumn) {
                tokens[i] = this.joinKeyString;
            } else
                tokens[i] = this.dontCareString;
        }
        return tokens;
    }

    public String[] constructUnionCrossConstraintTokens(TypeInformation typeInformation) {
        int totalFields = typeInformation.getTotalFields();
        String[] tokens = new String[totalFields];
        for (int i = 0; i < totalFields; i++) {
            tokens[i] = this.dontCareString;
        }
        return tokens;
    }

    public void convertConstraintRecordToConcreteRecord(SingleOperator child, Tuple constraintRecord) throws Exception {
        //child = leaf , parent = basetable
        Map<SingleOperator, List> loadOperatorWithUnUsedExamples = new LinkedHashMap<SingleOperator, List>();
        SingleOperator parent = child.getParentOperators().get(0);
        //convert only if its a leaf operator
        if (parent.getOperator() instanceof GenericDataSourceBase) {
            // System.out.println("UNUSED-----");
            List unUsedExamplesAtLeaf = getUnusedExamplesFromBaseTable(parent, child, child.getOperatorOutputAsList());
            loadOperatorWithUnUsedExamples.put(child, unUsedExamplesAtLeaf);
            if (!unUsedExamplesAtLeaf.isEmpty()) {
                for (int i = 0; i < constraintRecord.getArity(); i++) {
                    if (constraintRecord.getField(i) == this.joinKeyString ||
                            constraintRecord.getField(i) == this.joinKeyInteger ||
                            constraintRecord.getField(i) == this.joinKeyLong ||
                            constraintRecord.getField(i) == this.joinKeyDouble) {
                        Random random = new Random();
                        //todo recheck with multi joins
                        if (this.joinKey == null)
                            this.joinKey = ((Tuple) returnRandomTuple(unUsedExamplesAtLeaf, random)).getField(i);
                        constraintRecord.setField(this.joinKey, i);
                    }
                    if (constraintRecord.getField(i) == this.dontCareString ||
                            constraintRecord.getField(i) == this.dontCareDouble ||
                            constraintRecord.getField(i) == this.dontCareLong ||
                            constraintRecord.getField(i) == this.dontCareInteger) {
                        Random random = new Random();
                        Object randomValue = ((Tuple) returnRandomTuple(unUsedExamplesAtLeaf, random)).getField(i);
                        constraintRecord.setField(randomValue, i);

                    }

                }
            }
            // System.out.println("-------------------Changed Constraint Record-----------" + constraintRecord);
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

    //child operator is parent of operator with empty eq class
    public void propagateConstraintRecordUpstream(SingleOperator childOperator, Tuple constraintRecord) throws Exception {
        for (SingleOperator parent : childOperator.getParentOperators()) {
            if (childOperator.getOperatorType() != OperatorType.LOAD) {
                while (parent.getOperatorType() != OperatorType.LOAD) {
                    parent = parent.getParentOperators().get(0); //todo what if parent is dualinputoperator
                    parent.getOperatorOutputAsList().add(constraintRecord);
                    parent.setConstraintRecords(constraintRecord);
                    this.operatorToConstraintRecordMap.put(parent, constraintRecord);
                }

                parent.getOperatorOutputAsList().add(constraintRecord);
                parent.setConstraintRecords(constraintRecord);
                this.operatorToConstraintRecordMap.put(parent, constraintRecord);
                //parent is LOAD, once load is reached change to concrete
                convertConstraintRecordToConcreteRecord(parent, constraintRecord);
            }
            //if childOperator itself is load
            convertConstraintRecordToConcreteRecord(childOperator, constraintRecord);
        }
    }

    public void pruneTuples() throws Exception {

        for (int ctr = operatorTree.size(); ctr > 0; ctr--) {

            SingleOperator operator = operatorTree.get(ctr - 1);

            //not source
            if (operator.getEquivalenceClasses() != null) {
                for (EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()) {
                    if (equivalenceClass.hasExample() && equivalenceClass.getExamples().size() > 1) {
                        //remove one by one from upstream operators, while removing from upstream operator
                        //check the equivalence class of that operator is still maintained and not empty
                        for (int i = 0; i < operator.getOperatorOutputAsList().size(); i++) {
                            Object exampleToPrune = operator.getOperatorOutputAsList().get(i);
                            for (LinkedHashMap<SingleOperator, Object> recordTracer : this.lineageTracker.values()) {
                                if (recordTracer.values().contains(exampleToPrune)) {
                                    checkPruningIsOK(recordTracer);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void checkPruningIsOK(LinkedHashMap<SingleOperator, Object> recordTracer) throws Exception {
        LinkedList<SingleOperator> operatorList = new LinkedList<SingleOperator>(recordTracer.keySet());
        for (int i = 1; i <= operatorList.size(); i++) {
            SingleOperator followingOperator = null;
            SingleOperator operator = operatorList.get(operatorList.size() - i);
            if (i != 1)
                followingOperator = operatorList.get(operatorList.size() - i + 1);

            Object exampleUnderScrutiny = recordTracer.get(operator);
            //remove all instances of the example from the operator
            operator.getOperatorOutputAsList().removeAll(Collections.singleton(exampleUnderScrutiny));
            setOperatorEquivalenceClassess(operator);
            if (followingOperator != null ) {
                if (!checkEquivalenceClasses(operator) || !checkFollowingOperatorsEquivalenceClasses(operator, followingOperator, exampleUnderScrutiny))
                    operator.getOperatorOutputAsList().add(exampleUnderScrutiny);

            } else if (!checkEquivalenceClasses(operator))
                operator.getOperatorOutputAsList().add(exampleUnderScrutiny);
        }
    }

    public boolean checkFollowingOperatorsEquivalenceClasses(SingleOperator operator, SingleOperator followingOperator, Object exampleUnderScrutiny) throws Exception {
        if (followingOperator != null) {
            Operator operatorToConsider = followingOperator.getOperator();
            List input1 = new ArrayList();
            List input2 = new ArrayList();
            List output = new ArrayList();
            List prevOutput = followingOperator.getOperatorOutputAsList();

            for (int i = 0; i < followingOperator.getParentOperators().size(); i++) {
                SingleOperator parent = followingOperator.getParentOperators().get(i);
                if (parent == operator) {
                    if (i == 0) {
                        input1 = parent.getOperatorOutputAsList();
                    }
                    if (i == 1) {
                        input2 = parent.getOperatorOutputAsList();
                    }
                }
            }

            if (operatorToConsider instanceof DualInputOperator) {
                if (input1.isEmpty())
                    input1 = followingOperator.getParentOperators().get(0).getOperatorOutputAsList();
                if (input2.isEmpty())
                    input2 = followingOperator.getParentOperators().get(1).getOperatorOutputAsList();

                output = ((DualInputOperator) operatorToConsider).executeOnCollections(input1, input2, null, this.env.getConfig());
            }

            if (operatorToConsider instanceof SingleInputOperator) {
                if (input1.isEmpty())
                    input1 = followingOperator.getParentOperators().get(0).getOperatorOutputAsList();

                output = ((SingleInputOperator) operatorToConsider).executeOnCollections(input1, null, this.env.getConfig());
            }

            followingOperator.setOperatorOutputAsList(output);
            setOperatorEquivalenceClassess(followingOperator);
            if (!checkEquivalenceClasses(followingOperator) || !checkFollowingOperatorsEquivalenceClasses(followingOperator, getFollowingOperator(followingOperator), exampleUnderScrutiny)) {
                followingOperator.setOperatorOutputAsList(prevOutput);
                return false;
            } else
                return true;
        }
        return (checkEquivalenceClasses(operator));

    }

    public SingleOperator getFollowingOperator(SingleOperator prevOperator) {
        for (int i = 0; i < this.operatorTree.size(); i++) {
            if (this.operatorTree.get(i).getOperatorType() != OperatorType.SOURCE) {
                for (SingleOperator parent : this.operatorTree.get(i).getParentOperators()) {
                    if (parent == prevOperator)
                        return this.operatorTree.get(i);
                }
            }
        }
        return null;
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
            setOperatorEquivalenceClassess(operator);
        }
    }

    public void setOperatorEquivalenceClassess(SingleOperator operator) {
        if (operator.getOperatorType() == OperatorType.LOAD) {
            List loadExamples = operator.getOperatorOutputAsList();
            SingleEquivalenceClass loadEquivalenceClass = new SingleEquivalenceClass();

            if (!loadExamples.isEmpty()) {
                loadEquivalenceClass.getSingleExample().setHasExample(true);
                loadEquivalenceClass.getSingleExample().setExamples(loadExamples);
            } else
                loadEquivalenceClass.getSingleExample().setHasExample(false);

            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(loadEquivalenceClass.getSingleExample());
            loadEquivalenceClass.setSingleExample(loadEquivalenceClass.getSingleExample());
            operator.setEquivalenceClasses(equivalenceClasses);
        }

        if (operator.getOperatorType() == OperatorType.DISTINCT) {
            List distinctExamples = operator.getOperatorOutputAsList();
            SingleEquivalenceClass distinctEquivalenceClass = new SingleEquivalenceClass();

            if (!distinctExamples.isEmpty()) {
                distinctEquivalenceClass.getSingleExample().setHasExample(true);
                distinctEquivalenceClass.getSingleExample().setExamples(distinctExamples);
            } else
                distinctEquivalenceClass.getSingleExample().setHasExample(false);

            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(distinctEquivalenceClass.getSingleExample());
            distinctEquivalenceClass.setSingleExample(distinctEquivalenceClass.getSingleExample());
            operator.setEquivalenceClasses(equivalenceClasses);
        }

        if (operator.getOperatorType() == OperatorType.PROJECT) {
            List projectExamples = operator.getOperatorOutputAsList();
            SingleEquivalenceClass projectEquivalenceClass = new SingleEquivalenceClass();

            if (!projectExamples.isEmpty()) {
                projectEquivalenceClass.getSingleExample().setHasExample(true);
                projectEquivalenceClass.getSingleExample().setExamples(projectExamples);
            } else
                projectEquivalenceClass.getSingleExample().setHasExample(false);

            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(projectEquivalenceClass.getSingleExample());
            projectEquivalenceClass.setSingleExample(projectEquivalenceClass.getSingleExample());
            operator.setEquivalenceClasses(equivalenceClasses);
        }


        if (operator.getOperatorType() == OperatorType.JOIN) {
            List joinExamples = operator.getOperatorOutputAsList();
            JoinEquivalenceClasses joinEquivalenceClass = new JoinEquivalenceClasses();
            if (!joinExamples.isEmpty()) {
                joinEquivalenceClass.getJoinedExample().setHasExample(true);
                joinEquivalenceClass.getJoinedExample().setExamples(joinExamples);
            } else
                joinEquivalenceClass.getJoinedExample().setHasExample(false);


            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(joinEquivalenceClass.getJoinedExample());
            joinEquivalenceClass.setJoinedExample(joinEquivalenceClass.getJoinedExample());
            operator.setEquivalenceClasses(equivalenceClasses);

        }

        if (operator.getOperatorType() == OperatorType.UNION) {
            List firstParentExamples = operator.getParentOperators().get(0).getOperatorOutputAsList();
            List secondParentExamples = operator.getParentOperators().get(1).getOperatorOutputAsList();
            List operatorOutput = operator.getOperatorOutputAsList();
            UnionCrossEquivalenceClasses unionCrossEquivalenceClasses = new UnionCrossEquivalenceClasses();

            if (!operatorOutput.isEmpty()) {
                if (!firstParentExamples.isEmpty()) {
                    List parentExamples = new ArrayList();
                    for (Object firstParentExample : firstParentExamples) {
                        if (operatorOutput.contains(firstParentExample)) {
                            unionCrossEquivalenceClasses.getFirstTableExample().setHasExample(true);
                            parentExamples.add(firstParentExample);
                        }
                    }
                    if (parentExamples.isEmpty())
                        unionCrossEquivalenceClasses.getFirstTableExample().setHasExample(false);

                    unionCrossEquivalenceClasses.getFirstTableExample().setExamples(parentExamples);
                }

                if (!secondParentExamples.isEmpty()) {
                    List parentExamples = new ArrayList();
                    for (Object secondParentExample : secondParentExamples) {
                        if (operatorOutput.contains(secondParentExample)) {
                            unionCrossEquivalenceClasses.getSecondTableExample().setHasExample(true);
                            parentExamples.add(secondParentExample);
                        }
                    }
                    if (parentExamples.isEmpty())
                        unionCrossEquivalenceClasses.getSecondTableExample().setHasExample(false);

                    unionCrossEquivalenceClasses.getSecondTableExample().setExamples(parentExamples);
                }

            }
            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(unionCrossEquivalenceClasses.getFirstTableExample());
            equivalenceClasses.add(unionCrossEquivalenceClasses.getSecondTableExample());
            unionCrossEquivalenceClasses.setFirstTableExample(unionCrossEquivalenceClasses.getFirstTableExample());
            unionCrossEquivalenceClasses.setSecondTableExample(unionCrossEquivalenceClasses.getSecondTableExample());
            operator.setEquivalenceClasses(equivalenceClasses);
        }

        if (operator.getOperatorType() == OperatorType.CROSS) {

            List firstParentExamples = operator.getParentOperators().get(0).getOperatorOutputAsList();
            List secondParentExamples = operator.getParentOperators().get(1).getOperatorOutputAsList();
            List operatorOutput = operator.getOperatorOutputAsList();
            UnionCrossEquivalenceClasses unionCrossEquivalenceClasses = new UnionCrossEquivalenceClasses();

            if (!operatorOutput.isEmpty()) {
                if (!firstParentExamples.isEmpty()) {
                    List parentExamples = new ArrayList();
                    for (Object outputExample : operatorOutput) {
                        for (Object firstParentExample : firstParentExamples) {
                            if (checkCrossTokens((Tuple) outputExample, (Tuple) firstParentExample)) {
                                unionCrossEquivalenceClasses.getFirstTableExample().setHasExample(true);
                                parentExamples.add(firstParentExample);
                            }
                        }
                    }
                    if (parentExamples.isEmpty())
                        unionCrossEquivalenceClasses.getFirstTableExample().setHasExample(false);

                    unionCrossEquivalenceClasses.getFirstTableExample().setExamples(parentExamples);
                }

                if (!secondParentExamples.isEmpty()) {
                    List parentExamples = new ArrayList();
                    for (Object outputExample : operatorOutput) {
                        for (Object secondParentExample : secondParentExamples) {
                            if (checkCrossTokens((Tuple) outputExample, (Tuple) secondParentExample)) {
                                unionCrossEquivalenceClasses.getSecondTableExample().setHasExample(true);
                                parentExamples.add(secondParentExample);
                            }
                        }
                    }
                    if (parentExamples.isEmpty())
                        unionCrossEquivalenceClasses.getSecondTableExample().setHasExample(false);

                    unionCrossEquivalenceClasses.getSecondTableExample().setExamples(parentExamples);
                }

            }
            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(unionCrossEquivalenceClasses.getFirstTableExample());
            equivalenceClasses.add(unionCrossEquivalenceClasses.getSecondTableExample());
            unionCrossEquivalenceClasses.setFirstTableExample(unionCrossEquivalenceClasses.getFirstTableExample());
            unionCrossEquivalenceClasses.setSecondTableExample(unionCrossEquivalenceClasses.getSecondTableExample());
            operator.setEquivalenceClasses(equivalenceClasses);
        }

        if(operator.getOperatorType() == OperatorType.FILTER){
            FilterEquivalenceClasses filterEquivalenceClasses = new FilterEquivalenceClasses();
            List parentExamples = operator.getParentOperators().get(0).getOperatorOutputAsList();
            List filterExamples =  operator.getOperatorOutputAsList();
            List passExamples = new ArrayList();
            List failExamples = new ArrayList();

            for(Object parentExample : parentExamples){
                if(filterExamples.contains(parentExample))
                    passExamples.add(parentExample);
                else
                    failExamples.add(parentExample);
            }

            if(!passExamples.isEmpty()){
                filterEquivalenceClasses.getFilterPass().setHasExample(true);
                filterEquivalenceClasses.getFilterPass().setExamples(passExamples);
            }
            else
                filterEquivalenceClasses.getFilterPass().setHasExample(false);

            if(!failExamples.isEmpty()){
                filterEquivalenceClasses.getFilterFail().setHasExample(true);
                filterEquivalenceClasses.getFilterFail().setExamples(failExamples);
            }
            else
                filterEquivalenceClasses.getFilterFail().setHasExample(false);

            List<EquivalenceClass> equivalenceClasses = new ArrayList<EquivalenceClass>();
            equivalenceClasses.add(filterEquivalenceClasses.getFilterPass());
            equivalenceClasses.add(filterEquivalenceClasses.getFilterFail());
            filterEquivalenceClasses.setFilterPass(filterEquivalenceClasses.getFilterPass());
            filterEquivalenceClasses.setFilterFail(filterEquivalenceClasses.getFilterFail());
            operator.setEquivalenceClasses(equivalenceClasses);

        }
    }

    public boolean checkCrossTokens(Tuple outputExample, Tuple parentExample) {
        boolean[] isParentTokensPresent = new boolean[parentExample.getArity()];
        for (int ctr = 0; ctr < isParentTokensPresent.length; ctr++)
            isParentTokensPresent[ctr] = false;

        for (int i = 0; i < parentExample.getArity(); i++) {
            for (int j = 0; j < outputExample.getArity(); j++) {
                if (parentExample.getField(i) == outputExample.getField(j))
                    isParentTokensPresent[i] = true;
            }
        }

        for (int k = 0; k < isParentTokensPresent.length; k++)
            if (isParentTokensPresent[k] == false)
                return false;

        return true;
    }


    public boolean checkEquivalenceClasses(SingleOperator operator) {
        /*if(operator.getOperatorType() == OperatorType.FILTER)
            return true;*/

        boolean allEquivalenceClassCheck = true;

        if (operator.getEquivalenceClasses() != null) {
            for (EquivalenceClass equivalenceClass : operator.getEquivalenceClasses()) {
                if (!equivalenceClass.hasExample() || equivalenceClass.getExamples().size() < 1) {
                    allEquivalenceClassCheck = false;
                }
            }
        }
       /* if(operator.getEquivalenceClasses() == null)
            allEquivalenceClassCheck = false;*/

        return allEquivalenceClassCheck;
    }


    public Tuple getConstraintRecord(SingleOperator operator, List tokens) throws IllegalAccessException, InstantiationException {

        Tuple constraintRecord = drillToBasicType(operator.getOperatorOutputType(), tokens);
        return constraintRecord;
    }


    //Todo : check for all types (float,string...)
    public Tuple drillToBasicType(TypeInformation typeInformation, List tokens) throws IllegalAccessException, InstantiationException {
        Tuple valueSetTuple = (Tuple) typeInformation.getTypeClass().newInstance();

        for (int ctr = 0; ctr < typeInformation.getArity(); ctr++) {

            if (((CompositeType) typeInformation).getTypeAt(ctr).isTupleType())
                valueSetTuple.setField(drillToBasicType(((CompositeType) typeInformation).getTypeAt(ctr), tokens), ctr);

            else {
                String name = ((CompositeType) typeInformation).getTypeAt(ctr).toString();

                //valueSetTuple.setField(tokens.get(0), ctr);
                //tokens.remove(tokens.get(0));
                if (tokens.get(0) == this.dontCareString) {
                    if (name.equalsIgnoreCase("String"))
                        valueSetTuple.setField(this.dontCareString, ctr);
                    if (name.equalsIgnoreCase("Long"))
                        valueSetTuple.setField(this.dontCareLong, ctr);
                    if (name.equalsIgnoreCase("Integer"))
                        valueSetTuple.setField(this.dontCareInteger, ctr);
                    if (name.equalsIgnoreCase("Double"))
                        valueSetTuple.setField(this.dontCareDouble, ctr);
                }
                if (tokens.get(0) == this.joinKeyString) {
                    if (name.equalsIgnoreCase("String"))
                        valueSetTuple.setField(this.joinKeyString, ctr);
                    if (name.equalsIgnoreCase("Long"))
                        valueSetTuple.setField(this.joinKeyLong, ctr);
                    if (name.equalsIgnoreCase("Integer"))
                        valueSetTuple.setField(this.joinKeyInteger, ctr);
                    if (name.equalsIgnoreCase("Double"))
                        valueSetTuple.setField(this.joinKeyDouble, ctr);
                }
                tokens.remove(tokens.get(0));

            }
        }
        return valueSetTuple;
    }

}
