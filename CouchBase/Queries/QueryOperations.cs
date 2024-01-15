using Couchbase;
using Couchbase.Query;
using Newtonsoft.Json;

namespace CouchBase.Queries;

public class QueryOperations
{
    readonly ICluster _cluster;

    public QueryOperations(string conn, string user, string pwd)
    {
        _cluster = Cluster.ConnectAsync(
            connectionString: conn,
            username: user,
            password: pwd
            ).GetAwaiter().GetResult();
    }

    public async Task<IQueryResult<dynamic>> PositionalParameterQuery(string key)
    {
        var result = await _cluster.QueryAsync<dynamic>(
            "SELECT t.* FROM `travel-sample` t WHERE t.type=$1",
            options =>
            {
                options.Parameter(key);
            }
        );

        return result;
    }

    public async Task<IQueryResult<Landmark>> NamedParameterQuery(string key)
    {
        var result = await _cluster.QueryAsync<Landmark>(
            "SELECT t.* FROM `travel-sample` t WHERE t.type=$type",
            options => options.Parameter("type", key)
        );

        return result;
    }

    public async Task HandleResult(IQueryResult<dynamic> result)
    {
        if (result.MetaData.Status != QueryStatus.Success) return;

        await foreach (var row in result)
        {
            var name = row.name;
            var address = row.address;
            await Console.Out.WriteLineAsync($"{name}, {address}");
        }
    }
    public async Task HandleResult(IQueryResult<Landmark> result)
    {
        if (result.MetaData.Status != QueryStatus.Success) return;

        await foreach (var row in result)
        {
            var name = row.Name;
            var address = row.Address;
            await Console.Out.WriteLineAsync($"{name}, {address}");
        }
    }

    public async Task ScanConsistency()
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var upsertResult = await collection.UpsertAsync("doc1", new { name = "Mike AtPlus", type = "user" });

        var result = await _cluster.QueryAsync<dynamic>(
            "SELECT t.* FROM `travel-sample` t WHERE t.type=$1",
            options => options
                        .Parameter("user")
                        .ScanConsistency(QueryScanConsistency.NotBounded)
                        //.ScanConsistency(QueryScanConsistency.RequestPlus)
        );
    }

    public async Task ScanConsistencyAtPlus()
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var upsertResult = await collection.UpsertAsync("doc1", new { name = "Mike AtPlus", type = "user" });

        // create mutation state from mutation results
        var state = MutationState.From(upsertResult);

        // use mutation state with query option
        var result = await _cluster.QueryAsync<dynamic>(
            "SELECT t.* FROM `travel-sample` t WHERE t.type=$1",
            options => options
                        .Parameter("user")
                        .ConsistentWith(state)
        );
    }

    public async Task ReadonlyQuery()
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("inventory");

        var collection = await scope.CollectionAsync("users");

        var queryResult = await scope.QueryAsync<dynamic>("select * from airline LIMIT 10", options =>
        {
            options.Readonly(true); // Ensure that no data is mutated when this query is being parsed and planned => no state-mutating side effects
        });

        await foreach (var row in queryResult)
        {
            Console.WriteLine(row);
        }
    }

    public async Task ScopeLevelQuery() // Query at the scope level
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("inventory");

        var collection = await scope.CollectionAsync("users");

        var queryResult = await scope.QueryAsync<dynamic>("select * from airline LIMIT 10");

        await foreach (var row in queryResult)
        {
            Console.WriteLine(row);
        }
    }
}

public class Geo
{
    [JsonProperty("accuracy")]
    public string Accuracy { get; set; }

    [JsonProperty("lat")]
    public double Lat { get; set; }

    [JsonProperty("lon")]
    public double Lon { get; set; }
}

public class Landmark
{
    [JsonProperty("activity")]
    public string Activity { get; set; }

    [JsonProperty("address")]
    public string Address { get; set; }

    [JsonProperty("alt")]
    public object Alt { get; set; }

    [JsonProperty("city")]
    public string City { get; set; }

    [JsonProperty("content")]
    public string Content { get; set; }

    [JsonProperty("country")]
    public string Country { get; set; }

    [JsonProperty("directions")]
    public object Directions { get; set; }

    [JsonProperty("email")]
    public string Email { get; set; }

    [JsonProperty("geo")]
    public Geo Geo { get; set; }

    [JsonProperty("hours")]
    public string Hours { get; set; }

    [JsonProperty("id")]
    public int Id { get; set; }

    [JsonProperty("image")]
    public object Image { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("phone")]
    public string Phone { get; set; }

    [JsonProperty("price")]
    public object Price { get; set; }

    [JsonProperty("state")]
    public object State { get; set; }

    [JsonProperty("title")]
    public string Title { get; set; }

    [JsonProperty("tollfree")]
    public object TollFree { get; set; }

    [JsonProperty("type")]
    public string Type { get; set; }

    [JsonProperty("url")]
    public string Url { get; set; }
}