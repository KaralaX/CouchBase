using Couchbase;
using Couchbase.KeyValue;

namespace CouchBase.DataOperations;

public class AtomicCounters
{
    readonly ICluster _cluster;

    public AtomicCounters(string conn, string user, string pwd)
    {
        _cluster = Cluster.ConnectAsync(
            connectionString: conn,
            username: user,
            password: pwd
            ).GetAwaiter().GetResult();
    }
    public async Task<ulong> Increment(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var result = await collection.Binary.IncrementAsync(key);

        return result.Content;
    }

    public async Task<ulong> IncrementWithOptions(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var result = await collection.Binary.IncrementAsync(key, options =>
        {
            options.Delta(1); // Step: 1
            options.Initial(1000); // Initial Value: 1000
            options.Timeout(TimeSpan.FromSeconds(10)); //underlying network timeout
        });

        return result.Content;
    }
    public async Task<ulong> Decrement(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var result = await collection.Binary.DecrementAsync(key);

        return result.Content;
    }

    public async Task<ulong> DecrementWithOptions(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var result = await collection.Binary.DecrementAsync(key,
        options =>
        {
            options.Delta(1); // Step: 1
            options.Initial(1000); // Initial Value: 1000
            options.Timeout(TimeSpan.FromSeconds(5)); //underlying network timeout
        }
        );

        return result.Content;
    }
}
