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

        _cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds(5)).GetAwaiter().GetResult();
    }

    public async Task<IEnumerable<IGetResult>> SequentialRead(int noOperations, int testId)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        var results = new List<IGetResult>();

        for (int i = 0; i < noOperations; i++)
        {
            var res = await collection.GetAsync($"test_{testId}_{i}", options => options.Timeout(TimeSpan.FromSeconds(5)));

            results.Add(res);
        }

        return results;
    }
    public async Task SequentialWrite(int noOperations, int testId, object document)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        for (int i = 0; i < noOperations; i++)
        {
            var key = $"test_{testId}_{i}";

            var result = await collection.UpsertAsync(key, document, options =>
            {
                options.Timeout(TimeSpan.FromSeconds(5));
            });
        }
    }

    public async Task<IEnumerable<IGetResult>> BulkRead(int noOperations, int testId)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        var tasks = new List<Task<IGetResult>>(512);

        var results = new List<IGetResult>();

        for (int i = 0; i < noOperations; i++)
        {
            var task = collection.GetAsync($"test_{testId}_{i}", options => options.Timeout(TimeSpan.FromSeconds(5)));

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

    public async Task BulkWrite(int noOperations, int testId, object record)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        var tasks = new List<Task<IMutationResult>>(512);

        for (int i = 0; i < noOperations; i++)
        {
            var task = collection.UpsertAsync(
                $"test_{testId}_{i}",
                record,
                options =>
                {
                    options.Timeout(TimeSpan.FromSeconds(5));
                }
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
