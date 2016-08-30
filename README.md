# streamjoin

... for SQL-like java 8 stream joins, inspired by C# Enumerable.Join().

Simple show room here: [JoinTest.java](https://github.com/simomat/streamjoin/blob/master/src/test/java/de/infonautika/streamjoin/JoinTest.java)

Ideas for next steps:
- [ ] add non-equi join (like 'WHERE A.RANK < B.RANK')
- [ ] additionally to grouper/combiner lambdas, let user pass a custom consumer for join matches
- [ ] for outer joins: let user define default values or factories instead passing 'null' to consumer
- [ ] make generic types less restrictive (use <? extends L> and so on)
- [ ] submit to JCenter
- [ ] get more ideas
