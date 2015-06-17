package flink.examplegeneration.input.operatortree;

/**
 * Enum to describe the operator type
 */
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
