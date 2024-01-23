using Couchbase;
using Couchbase.KeyValue;
using System.Text;
using System.Text.Json;



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
    }

    public async Task InitializeAsync()
    {
        await _cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds(5));
    }

    public async Task<IEnumerable<IGetResult>> SequentialRead(int noOperations, int testId)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        var results = new List<IGetResult>();

        for (int i = 0; i < noOperations; i++)
        {
            try
            {
                var res = await collection.GetAsync($"test_{testId}_{i}", options => options.Timeout(TimeSpan.FromSeconds(5)));
                results.Add(res);
            }
            catch (Exception)
            {
            }
        }

        return results;
    }
    public async Task SequentialWrite(int noOperations, int testId, object document)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var collection = await bucket.DefaultCollectionAsync();

        for (int i = 0; i < noOperations; i++)
        {
            try
            {
                var result = await collection.UpsertAsync($"test_{testId}_{i}", document, options =>
                {
                    options.Timeout(TimeSpan.FromSeconds(5));
                });
            }
            catch (Exception)
            {
            }

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

            if (tasks.Count >= 500)
            {
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (Exception)
                {
                }

                foreach (var t in tasks)
                {
                    results.Add(t.Result);
                }

                tasks.Clear();
            }
        }
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception)
        {
        }

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

            if (tasks.Count >= 500)
            {
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (Exception)
                {
                }

                tasks.Clear();
            }
        }
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception)
        {
        }
    }
    public async Task BulkReadSQL(int noOperations, int testId)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.DefaultScopeAsync();

        var count = 0;

        var builder = new StringBuilder("INSERT INTO _default (KEY, VALUE) VALUES ");

        for (int i = 0; i < noOperations; i++)
        {
            if (count++ >= 512)
            {
                await scope.QueryAsync<dynamic>(
                    statement: builder.ToString(),
                    options =>
                    {
                        options.PipelineBatch(1000);
                        options.Timeout(TimeSpan.FromSeconds(10));
                        options.MaxServerParallelism(1000);
                    }
                );
            }

            builder.AppendLine();
        }
    }
    public async Task BulkWriteSQL(int noOperations, int testId, string statement)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.DefaultScopeAsync();

        for (int i = 0; i < noOperations; i++)
        {
            await scope.QueryAsync<dynamic>(
                statement: statement,
                options =>
                {
                    options.PipelineBatch(500);
                    options.Timeout(TimeSpan.FromSeconds(20));
                }
            );
        }
    }
}
