using Couchbase;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Newtonsoft.Json;

namespace CouchBase.DataOperations;

public class CRUDOperations
{
    readonly ICluster _cluster;

    public CRUDOperations(string conn, string user, string pwd)
    {
        _cluster = Cluster.ConnectAsync(
            connectionString: conn,
            username: user,
            password: pwd
            ).GetAwaiter().GetResult();
    }

    public async Task Insert(string key, object document)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        try
        {
            var result = await collection.InsertAsync(key, document);
        }
        catch (DocumentExistsException)
        {
            await Console.Out.WriteLineAsync("Document already exists");
        }
    }

    public async Task InsertWithOptions(string key, object document)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        try
        {
            var result = await collection.InsertAsync(key, document, options =>
            {
                options.Expiry(TimeSpan.FromMinutes(1));
                options.Timeout(TimeSpan.FromSeconds(5));
            });
        }
        catch (DocumentExistsException)
        {
            await Console.Out.WriteLineAsync("Document already exists");
        }
    }

    public async Task ReplaceWithOptions(string key, object document)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var previousResult = await collection.GetAsync(key);

        var cas = previousResult.Cas;

        var result = await collection.ReplaceAsync(key, document, options =>
        {
            options.Cas(cas); // Compare and Swap (CAS) - representing the current state of the item
            options.Expiry(TimeSpan.FromMinutes(1));  // explicit time to live
            options.Timeout(TimeSpan.FromSeconds(5)); // network connection timeout
        });
    }

    public async Task Upsert(string key, object document)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        var result = await collection.UpsertAsync(key, document, options =>
        {
            options.Expiry(TimeSpan.FromMinutes(1));  // explicit time to live
            options.Durability(DurabilityLevel.Majority);
            options.Timeout(TimeSpan.FromSeconds(5)); // network connection timeout
        });
    }

    public async Task<Person?> Get(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");
        try
        {
            var result = await collection.GetAsync(key);

            return result.ContentAs<Person>();
        }
        catch (DocumentNotFoundException)
        {
            await Console.Out.WriteLineAsync("Document not found");
        }

        return null;
    }

    public async Task<Person?> GetWithOptions(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        try
        {
            var result = await collection.GetAsync(key, options =>
            {
                options.Timeout(TimeSpan.FromSeconds(5)); // network connection timeout
            });

            return result.ContentAs<Person>();
        }
        catch (DocumentNotFoundException)
        {
            await Console.Out.WriteLineAsync("Document not found");
        }

        return null;
    }

    public async Task Remove(string key)
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        try
        {
            var result = await collection.GetAsync(key);

            await collection.RemoveAsync(key, options =>
            {
                options.Cas(result.Cas);
                options.Timeout(TimeSpan.FromSeconds(5));
            });
        }
        catch (DocumentNotFoundException)
        {
            await Console.Out.WriteLineAsync("Document not found");
        }
    }
    public async Task Touch(string key) // TTL Set Method for document 
    {
        using var bucket = await _cluster.BucketAsync("travel-sample");

        var scope = await bucket.ScopeAsync("tenant_agent_00");

        var collection = await scope.CollectionAsync("users");

        await collection.TouchAsync(key, TimeSpan.FromSeconds(10));

        // Touch with options

        //await collection.TouchAsync(key, TimeSpan.FromSeconds(30), options =>
        //{
        //    options.Timeout(TimeSpan.FromSeconds(5));
        //});
    }

}
public class Address
{
    [JsonProperty("type")]
    public string Type { get; set; }

    [JsonProperty("address")]
    public string AddressValue { get; set; }

    [JsonProperty("city")]
    public string City { get; set; }

    [JsonProperty("country")]
    public string Country { get; set; }
    public override string ToString()
    {
        return $"Address [Type: {Type}, Address: {AddressValue}, City: {City}, Country: {Country}]";
    }
}

public class CreditCard
{
    [JsonProperty("type")]
    public string Type { get; set; }

    [JsonProperty("number")]
    public string Number { get; set; }

    [JsonProperty("expiration")]
    public string Expiration { get; set; }
    public override string ToString()
    {
        return $"CreditCard [Type: {Type}, Number: {Number}, Expiration: {Expiration}]";
    }
}

public class Person
{
    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("addresses")]
    public List<Address> Addresses { get; set; }

    [JsonProperty("driving_licence")]
    public string DrivingLicence { get; set; }

    [JsonProperty("passport")]
    public string Passport { get; set; }

    [JsonProperty("preferred_email")]
    public string PreferredEmail { get; set; }

    [JsonProperty("preferred_phone")]
    public string PreferredPhone { get; set; }

    [JsonProperty("preferred_airline")]
    public string PreferredAirline { get; set; }

    [JsonProperty("preferred_airport")]
    public string PreferredAirport { get; set; }

    [JsonProperty("credit_cards")]
    public List<CreditCard> CreditCards { get; set; }

    [JsonProperty("created")]
    public DateTime Created { get; set; }

    [JsonProperty("updated")]
    public DateTime Updated { get; set; }
    public override string ToString()
    {
        return $"Person [Name: {Name}, Email: {PreferredEmail}, Addresses: {string.Join(", ", Addresses)}, " +
               $"Driving Licence: {DrivingLicence}, Passport: {Passport}, " +
               $"Preferred Airline: {PreferredAirline}, Preferred Airport: {PreferredAirport}, " +
               $"Credit Cards: {string.Join(", ", CreditCards)}, Created: {Created}, Updated: {Updated}]";
    }
}