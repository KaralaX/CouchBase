using CouchBase.Queries;
using System.Diagnostics;
using Xunit.Abstractions;

namespace CouchBase_Specs.Queries;

public class QueryOperations_Specs
{
    readonly QueryOperations _operations;

    readonly ITestOutputHelper _helper;

    public QueryOperations_Specs(ITestOutputHelper helper)
    {
        _operations = new QueryOperations(
            conn: "couchbase://localhost",
            user: "Administrator",
            pwd: "g3JRuZPng!K_XT"
            );
        _helper = helper;
    }

    [Fact]
    public async void PositionalParameterQuerySpecs()
    {
        //Arrange
        var key = "landmark";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        var result = await _operations.PositionalParameterQuery(key);

        await _operations.HandleResult(result);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"PositionalParameterQuery Client took {stopwatch.ElapsedMilliseconds} ms");

        _helper.WriteLine($"PositionalParameterQuery Server took {result.MetaData.Metrics.ExecutionTime} ms"); // using query metadata
    }
    [Fact]
    public async void NamedParameterQuerySpecs()
    {
        //Arrange
        var key = "3f78476a-a41f-452c-a5ae-00ed12657bc5";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        var result = await _operations.NamedParameterQuery(key);

        await _operations.HandleResult(result);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"NamedParameterQuery Client took {stopwatch.ElapsedMilliseconds} ms");

        _helper.WriteLine($"NamedParameterQuery Server took {result.MetaData.Metrics.ExecutionTime} ms"); // using query metadata
    }

    [Fact]
    public async void ScanConsistencySpecs()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.ScanConsistency();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"ScanConsistency took {stopwatch.ElapsedMilliseconds} ms");
    }
    
    [Fact]
    public async void ScanConsistencyAtPlusSpecs()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.ScanConsistencyAtPlus();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"ScanConsistencyAtPlus took {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async void ReadonlyQuery()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.ReadonlyQuery();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"ReadonlyQuery took {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async void ScopeLevelQuery()
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.ScopeLevelQuery();

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"ScopeLevelQuery took {stopwatch.ElapsedMilliseconds} ms");
    }
}
