using Couchbase.Core.Exceptions.KeyValue;
using CouchBase.DataOperations;
using System.Diagnostics;
using Xunit.Abstractions;

namespace CouchBase_Specs.DataOperations;

public class CRUDOperations_Specs
{
    readonly CRUDOperations _operations;

    readonly ITestOutputHelper _helper;

    public CRUDOperations_Specs(ITestOutputHelper helper)
    {
        _operations = new CRUDOperations(
            conn: "couchbase://localhost",
            user: "Administrator",
            pwd: "g3JRuZPng!K_XT");

        _helper = helper;
    }

    [Fact]
    public async void InsertSpecs()
    {
        //Arrange
        var key = Guid.NewGuid().ToString();
        var document = new { foo = "bar", bar = "foo" };

        //Acts
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.Insert(key, document);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Insert took {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async void InsertWithOptionsSpecs()
    {
        //Arrange
        var key = Guid.NewGuid().ToString();
        var document = new { foo = "bar", bar = "foo" };

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.InsertWithOptions(key, document);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"InsertWithOptions took {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async void ReplaceWithOptionsSpecs()
    {
        //Arrange
        var key = "enter-id";
        var document = new { foo = "bar", bar = "foo", bruh = "lmao" };

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.ReplaceWithOptions(key, document);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"ReplaceWithOptions took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void UpsertSpecs() // Up/date or In/sert
    {
        //Arrange
        var key = "e850bba3-397f-49b9-aa39-71901fa9d2eb";
        //var key = Guid.NewGuid().ToString();
        var document = new { foo = "bar", bar = "foo", bruh = "lmao" };

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.Upsert(key, document);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Upsert took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void GetSpecs()
    {
        //Arrange
        var key = "0";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        var result = await _operations.Get(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Get took {stopwatch.ElapsedMilliseconds} ms");

        _helper.WriteLine($"Item : {result}");
    }

    [Fact]
    public async void GetWithOptionsSpecs()
    {
        //Arrange
        var key = "1";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        var result = await _operations.GetWithOptions(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"GetWithOptions took {stopwatch.ElapsedMilliseconds} ms");

        _helper.WriteLine($"Item : {result}");
    }

    [Fact]
    public async void RemoveSpecs()
    {
        //Arrange
        var key = "ec7f49d1-7823-4d06-84d9-d6c8c4959fa1";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.Remove(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Remove took {stopwatch.ElapsedMilliseconds} ms");
    }
    [Fact]
    public async void TouchSpecs()
    {
        //Arrange
        var key = "3f78476a-a41f-452c-a5ae-00ed12657bc5";

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.Touch(key);

        stopwatch.Stop();

        //Assert
        _helper.WriteLine($"Touch took {stopwatch.ElapsedMilliseconds} ms");
    }

}