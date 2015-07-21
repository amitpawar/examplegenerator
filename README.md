Example Generator in Apache Flink 0.9.0

For a give dataflow, we generate set of concise and complete examples after each operation for a better understanding of the complete dataflow, as this allows user to verify and validate the behaviour of the given operator. 

Example generator output for a dataflow example that involves LOAD (2 datasets UserVisits <UserName, URL> and PageRank <URL, Rank> ) and JOIN on URL to get the rank is displayed below.

-----------------------------------------------------------------------------------------
LOAD FlatMap at testJoinWithEmptyEquivalenceClass(TestJoinWEmptyEquivalenceClass.java:25)
-----------------------------------------------------------------------------------------
| MNO  || https://www.fb.com  |**
-----------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
DISTINCT Distinct at testJoinWithEmptyEquivalenceClass(TestJoinWEmptyEquivalenceClass.java:25)
------------------------------------------------------------------------------------------
| MNO  || https://www.fb.com  |
------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
LOAD FlatMap at testJoinWithEmptyEquivalenceClass(TestJoinWEmptyEquivalenceClass.java:26)
-----------------------------------------------------------------------------------------
| https://www.fb.com  || 3  |**
-----------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
DISTINCT Distinct at testJoinWithEmptyEquivalenceClass(TestJoinWEmptyEquivalenceClass.java:26)
------------------------------------------------------------------------------------------
| https://www.fb.com  || 3  |
------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
JOIN Join at testJoinWithEmptyEquivalenceClass(TestJoinWEmptyEquivalenceClass.java:28)
--------------------------------------------------------------------------------------
| (MNO,https://www.fb.com)  || (https://www.fb.com,3)  |
--------------------------------------------------------------------------------------
** Synthetic Records
