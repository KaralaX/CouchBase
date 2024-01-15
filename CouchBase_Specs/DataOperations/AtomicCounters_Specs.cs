using CouchBase.DataOperations;
using System.Diagnostics;
using Xunit.Abstractions;

namespace CouchBase_Specs.DataOperations;

public class AtomicCounters_Specs
{
    readonly AtomicCounters _operations;

    readonly ITestOutputHelper _helper;

    public AtomicCounters_Specs(ITestOutputHelper helper)
    {
        _operations = new AtomicCounters(
            conn: "couchbase://localhost",
            user: "Administrator",
            pwd: "g3JRuZPng!K_XT");

        _helper = helper;
    }
    [Fact]
    public async void IncrementSpecs()
    {
        //Arrange
        var key = "3f78476a-a41f-452c-a5ae-00ed12657bc5";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.Increment(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Increment took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void IncrementWithOptionsSpecs()
    {
        //Arrange
        var key = Guid.NewGuid().ToString();

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.IncrementWithOptions(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"IncrementWithOptions took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void DecrementSpecs()
    {
        //Arrange
        var key = "99acb052-c9b6-4103-b6d2-ebec06b4fce6";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.Decrement(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Decrement took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void DecrementWithOptionsSpecs()
    {
        //Arrange
        var key = "99acb052-c9b6-4103-b6d2-ebec06b4fce6";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.DecrementWithOptions(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"DecrementWithOptions took {stopwatch.ElapsedMilliseconds} ms");
    }
}
