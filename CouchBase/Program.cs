using BenchmarkDotNet.Running;
using CouchBase.BatchProcessing;

var summary = BenchmarkRunner.Run<BenchMarkBulkSqlOperation>();