using Couchbase;
using Couchbase.KeyValue;

namespace CouchBase.BatchProcessing;

public class BatchOperation
{
    readonly ICluster _cluster;

    public BatchOperation(string conn, string user, string pwd)
    {
        _cluster = Cluster.ConnectAsync(
            connectionString: conn,
            username: user,
            password: pwd
            ).GetAwaiter().GetResult();

        _cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds().GetAwaiter().GetResult();
    }

    public async Task<IEnumerable<IGetResult>> BulkRead(int noOperations, int testId)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        var tasks = new List<Task<IGetResult>>(512);

        var results = new List<IGetResult>();

        for (int i = 0; i < noOperations; i++)
        {
            var task = collection.GetAsync($"test_{testId}_{i}");

            tasks.Add(task);

            if (tasks.Count >= 512)
            {
                await Task.WhenAll(tasks);

                foreach (var t in tasks)
                {
                    results.Add(t.Result);
                }

                tasks.Clear();
            }
        }

        await Task.WhenAll(tasks);

        foreach (var task in tasks)
        {
            results.Add(task.Result);
        }

        return results;
    }

    public async Task BulkWrite(int noOperations, int testId, dynamic record)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        var tasks = new List<Task<IMutationResult>>(512);

        for (int i = 0; i < noOperations; i++)
        {
            var task = collection.UpsertAsync(
                $"test_{testId}_{i}",
                record
                );

            tasks.Add(task);

            if (tasks.Count >= 512)
            {
                await Task.WhenAll(tasks);

                tasks.Clear();
            }
        }

        await Task.WhenAll(tasks);
    }
}
