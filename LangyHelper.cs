using Azure.Data.Tables;
using System;

namespace Langy
{
    internal static class LangyHelper
    {

        public static TableClient CreaTableClient()
        {
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            return table;
        }
    }
}