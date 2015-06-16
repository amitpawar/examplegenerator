package flink.examplegeneration.input.operatortree;

public enum OperatorType {
	LOAD,
	CROSS,
	DISTINCT,
	FILTER,
	JOIN,
	PROJECT,
	UNION,
	SOURCE,
	GROUPREDUCE,
	REDUCE,
	FLATMAP
}