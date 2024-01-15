using Couchbase.Management.Users;
using CouchBase.Search;
using System.Diagnostics;
using Xunit.Abstractions;

namespace CouchBase_Specs.Search;

public class SearchOperationSpecs
{
    readonly SearchOperations _operations;

    readonly ITestOutputHelper _helper;

    public SearchOperationSpecs(ITestOutputHelper helper)
    {
        _operations = new SearchOperations
        (
            conn: "couchbase://localhost",
            user: "Administrator",
            pwd: "g3JRuZPng!K_XT"
        );

        _helper = helper;
    }

    [Fact]
    public async void MatchQuerySpecs()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.MatchQuery();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"MatchQuery took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void DateRangeQuerySpecs()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.DateRangeQuery();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"DateRangeQuery took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void ConjunctionQuerySpecs()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.ConjunctionQuery();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"ConjunctionQuery took {stopwatch.ElapsedMilliseconds} ms");
    }
}
