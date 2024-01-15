using Couchbase;
using Couchbase.Search;
using Couchbase.Search.Queries.Compound;
using Couchbase.Search.Queries.Range;
using Couchbase.Search.Queries.Simple;

namespace CouchBase.Search;

public class SearchOperations
{
    readonly ICluster _cluster;

    public SearchOperations(string conn, string user, string pwd)
    {
        _cluster = Cluster.ConnectAsync(
            connectionString: conn,
            username: user,
            password: pwd
            ).GetAwaiter().GetResult();
    }

    public async Task<ISearchResult> MatchQuery()
    {
        return await _cluster.SearchQueryAsync(
            indexName: "landmark-content-index",
            query: new MatchQuery("+view"),
            options =>
            {
                options.Limit(10);
                options.ScanConsistency(SearchScanConsistency.AtPlus); // allow consistency
            }
        );
    }

    public async Task<ISearchResult> DateRangeQuery()
    {
        return await _cluster.SearchQueryAsync(
            indexName: "landmark-content-index",
            query: new DateRangeQuery()
                .Start(DateTime.Parse("2021-01-01"), true) // final Parameter is if the range is inclusive
                .End(DateTime.Parse("2021-02-01"), false),
            options =>
            {
                options.Limit(10);
            }
        );
    }

    public async Task<ISearchResult> ConjunctionQuery()
    {
        return await _cluster.SearchQueryAsync(
            indexName: "landmark-content-index",
            query: new ConjunctionQuery(
                new DateRangeQuery()
                    .Start(DateTime.Parse("2021-01-01"), true) // final Parameter is if the range is inclusive
                    .End(DateTime.Parse("2021-02-01"), false),
                new MatchQuery("+view")
            ),
            options => options.Limit(10)
        );
    }

    public async void HandleHits(ISearchResult result)
    {
        foreach(var hit in result.Hits)
        {
            await Console.Out.WriteLineAsync($"{hit.Fields} {hit.Id}");
        }
    }

    public async void HandleFacets(ISearchResult result)
    {
        foreach(var facet in result.Facets)
        {
            await Console.Out.WriteLineAsync($"Key: {facet.Key} - Value: {facet.Value}");
        }
    }
}
