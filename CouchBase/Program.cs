using Couchbase;
using CouchBase.Queries;
using CouchBase.Search;

//// Connect 
//var cluster = await Cluster.ConnectAsync(
//    connectionString: "couchbase://localhost", 
//    username: "Administrator", 
//    password: "g3JRuZPng!K_XT");

//await cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));

//// get a bucket reference
//var bucket = await cluster.BucketAsync("travel-sample");

//// Add and retreive documents

//// get a user-defined collection reference
//var scope = await bucket.ScopeAsync("tenant_agent_00");
//var collection = await scope.CollectionAsync("users");

//// upsert document
//var upsertResult = await collection.UpsertAsync("my-document-key", new { Name = "Ted", Age = 31 });
//var getResult = await collection.GetAsync("my-document-key");

//Console.WriteLine(getResult.ContentAs<dynamic>());


//// SQL++ Lookup
//// call the QueryAsync() function on the scope object and store the result
//var inventoryScope = bucket.Scope("inventory");
//var queryResult = await inventoryScope.QueryAsync<dynamic>("Select * from airline where id = 10");

////iterate over the rows to access result data and print to the terminal.
//await foreach(var row in queryResult)
//{
//    Console.WriteLine(row);
//}


//var operations = new QueryOperations(
//            conn: "couchbase://localhost",
//            user: "Administrator",
//            pwd: "g3JRuZPng!K_XT"
//            );

//var result1 = await operations.PositionalParameterQuery("landmark");

//await operations.HandleResult(result1);

//var result2 = await operations.NamedParameterQuery("3f78476a-a41f-452c-a5ae-00ed12657bc5");

//await operations.HandleResult(result2);


var operations = new SearchOperations(
    conn: "couchbase://localhost",
    user: "Administrator",
    pwd: "g3JRuZPng!K_XT"
    );


var result1 = await operations.MatchQuery();

operations.HandleHits(result1);

var result2 = await operations.DateRangeQuery();

operations.HandleFacets(result2);

var reuslt3 = await operations.ConjunctionQuery();

operations.HandleHits(reuslt3);