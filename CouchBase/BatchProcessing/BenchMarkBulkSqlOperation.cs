using BenchmarkDotNet.Attributes;
using System.Text;
using System.Text.Json;

namespace CouchBase.BatchProcessing;
[IterationCount(1)]
[WarmupCount(1)]
[MemoryDiagnoser]
public class BenchMarkBulkSqlOperation
{
    BatchOperation operation;

    [GlobalSetup]
    public async Task Setup()
    {
        operation = new BatchOperation(
            conn: "couchbase://localhost",
            user: "admin",
            pwd: "CRU6Hh8Tfn@kVg"
            );
    }

    [Params(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)]
    public int TestId { get; set; }

    [Benchmark]
    public async Task BulkWrite()
    {
        await operation.InitializeAsync();
        object document = null;

        var noOperations = 0;
        switch (TestId)
        {
            case <= 4:
                noOperations = 1000;
                switch (TestId)
                {
                    case 1:
                        document = InitializeDocument(1);
                        break;
                    case 2:
                        document = InitializeDocument(128);
                        break;
                    case 3:
                        document = InitializeDocument(512);
                        break;
                    case 4:
                        document = InitializeDocument(1024);
                        break;
                }
                break;
            case <= 8:
                noOperations = 10000;
                switch (TestId)
                {
                    case 5:
                        document = InitializeDocument(1);
                        break;
                    case 6:
                        document = InitializeDocument(128);
                        break;
                    case 7:
                        document = InitializeDocument(512);
                        break;
                    case 8:
                        document = InitializeDocument(1024);
                        break;
                }
                break;
            case <= 12:
                noOperations = 100000;
                switch (TestId)
                {
                    case 9:
                        document = InitializeDocument(1);
                        break;
                    case 10:
                        document = InitializeDocument(128);
                        break;
                    case 11:
                        document = InitializeDocument(512);
                        break;
                    case 12:
                        document = InitializeDocument(1024);
                        break;
                }
                break;
            case <= 16:
                switch (TestId)
                {
                    case 13:
                        document = InitializeDocument(1);
                        break;
                    case 14:
                        document = InitializeDocument(128);
                        break;
                    case 15:
                        document = InitializeDocument(512);
                        break;
                    case 16:
                        document = InitializeDocument(1024);
                        break;
                }
                noOperations = 1000000;
                break;
        }

        var payload = JsonSerializer.Serialize(document);

        var builder = new StringBuilder("UPSERT INTO _default (KEY, VALUE)");

        for (int i = 0; i < 500; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }
            builder.AppendLine($"VALUES (\"test_{TestId}_{i}\", {payload})");
        }

        await operation.BulkWriteSQL(noOperations/500, TestId, builder.ToString());
    }
    private dynamic InitializeDocument(int payloadSize)
    {
        dynamic document = "";

        if (payloadSize == 1)
        {
            document = "1";
        }
        if (payloadSize == 128)
        {
            document = new
            {
                title = "Gillingham (Kent)",
                name = "Medway Youth Hostel",
                address = "Capstone Road, ME7 3JE",
                directions = ""
            };
        }
        if (payloadSize == 512)
        {
            document = new
            {
                title = "Gillingham (Kent)",
                name = "Medway Youth Hostel",
                address = "Capstone Road, ME7 3JE",
                directions = "",
                phone = "+44 870 770 5964",
                tollfree = "",
                email = "",
                fax = "",
                url = "http=//www.yha.org.uk",
                checkin = "",
                checkout = "",
                price = "",
                geo = new
                {
                    lat = 51.35785,
                    lon = 0.55818,
                    accuracy = "RANGE_INTERPOLATED"
                },
                type = "hotel",
                id = 10025,
                country = "United Kingdom",
                city = "Medway",
                state = ""
            };
        }
        if (payloadSize == 1024)
        {
            document = new
            {
                title = "Gillingham (Kent)",
                name = "Medway Youth Hostel",
                address = "Capstone Road, ME7 3JE",
                directions = "",
                phone = "+44 870 770 5964",
                tollfree = "",
                email = "",
                fax = "",
                url = "http=//www.yha.org.uk",
                checkin = "",
                checkout = "",
                price = "",
                geo = new
                {
                    lat = 51.35785,
                    lon = 0.55818,
                    accuracy = "RANGE_INTERPOLATED"
                },
                type = "hotel",
                id = 10025,
                country = "United Kingdom",
                city = "Medway",
                state = "",
                reviews = new
                {
                    content = "This was our 2nd trip here and we enjoyed it as much or more than last year. Excellent location across from the French Market and just across the street from the streetcar stop. Very convenient to several small but good restaurants. Very clean and well maintained. Housekeeping and other staff are all friendly and helpful. We really enjoyed sitting on the 2nd floor terrace over the entrance and \"people-watching\" on Esplanade Ave., also talking with our fellow guests. Some furniture could use a little updating or replacement, but nothing major.",
                    ratings = new
                    {
                        Service = 5,
                        Cleanliness = 5,
                        Overall = 4,
                        Value = 4,
                        Locatio = 4,
                        Rooms = 3
                    },
                    author = "Ozella Sipes",
                    date = "2013-06-22 18:33:50 +0300"
                }
            };
        }
        return document;
    }
}
