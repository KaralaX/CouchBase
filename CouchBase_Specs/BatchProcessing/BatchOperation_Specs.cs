﻿using Couchbase.KeyValue;
using CouchBase.BatchProcessing;
using System.Diagnostics;
using Xunit.Abstractions;

namespace CouchBase_Specs.BatchProcessing;

public class BatchOperation_Specs
{
    readonly BatchOperation _operations;

    readonly ITestOutputHelper _helper;

    public BatchOperation_Specs(ITestOutputHelper helper)
    {
        _operations = new BatchOperation(
            conn: "couchbase://localhost",
            user: "admin",
            pwd: "CRU6Hh8Tfn@kVg"
            );
        _helper = helper;
    }


    [Theory]
    [InlineData(1000, 1)]
    [InlineData(10000, 5)]
    [InlineData(100000, 9)]
    [InlineData(1000000, 13)]
    public async Task BulkReadSpecs(int noOperations, int testId)
    {
        //Arrange

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        var result = await _operations.BulkRead(noOperations, testId);

        stopwatch.Stop();

        //Assert
        Assert.NotNull(result);

        Assert.Equal(noOperations, result.Count());

        _helper.WriteLine($"BulkRead took {stopwatch.ElapsedMilliseconds} ms to execute {noOperations} reads");
    }

    [Theory]
    [InlineData(1000, 1, 1)]
    [InlineData(1000, 128, 2)]
    [InlineData(1000, 512, 3)]
    [InlineData(1000, 1024, 4)]

    [InlineData(10000, 1, 5)]
    [InlineData(10000, 128, 6)]
    [InlineData(10000, 512, 7)]
    [InlineData(10000, 1024, 8)]

    [InlineData(100000, 1, 9)]
    [InlineData(100000, 128, 10)]
    [InlineData(100000, 512, 11)]
    [InlineData(100000, 1024, 12)]

    [InlineData(1000000, 1, 13)]
    [InlineData(1000000, 128, 14)]
    [InlineData(1000000, 512, 15)]
    [InlineData(1000000, 1024, 16)]
    public async Task BulkWriteSpecs(int noOperations, int payloadSize, int testId)
    {
        //Arrange
        dynamic document = InitializeDocument(payloadSize);

        //Act
        var stopwatch = new Stopwatch();

        stopwatch.Start();

        await _operations.BulkWrite(noOperations, testId, document);

        stopwatch.Stop();

        //Assert        
        _helper.WriteLine($"BulkWrite took {stopwatch.ElapsedMilliseconds} ms to execute {noOperations} writes with payload of {payloadSize} bytes");
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