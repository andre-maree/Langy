using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;

namespace Langy
{
    public class GroupObject
    {
        public string Group { get; set; }
        public List<string> Keys { get; set; }
    }

    public class MetaData
    {
        public List<GroupObject> Groups { get; set; }
        public List<string> Codes { get; set; }
    }

    public class Compile
    {
        private static readonly bool Compress = false;

        [FunctionName("Compile")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>();

            MetaData metas = await context.CallActivityAsync<MetaData>(nameof(GetMetaData), true);

            outputs.AddRange(metas.Codes);

            await context.CallActivityAsync(nameof(Process), metas);

            return outputs;
        }

        [FunctionName(nameof(Process))]
        public static async Task Process([ActivityTrigger] MetaData metas)
        {
            BlobContainerClient containerClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "langy-translations");
            containerClient.CreateIfNotExists();

            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");
            List<TableEntity> langents = new();

            foreach (var lang in metas.Codes)
            {
                AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{lang}'");

                langents.Clear();

                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    if(qEntity.Keys.Count > 5)
                    {
                        qEntity.RowKey = qEntity.GetString("Text");
                    }

                    langents.Add(qEntity);
                }

                foreach (var group in metas.Groups)
                {
                    Dictionary<string, string> values = new();

                    BlobClient blobClient = containerClient.GetBlobClient(lang + "-" + group.Group);

                    foreach (var id in group.Keys)
                    {
                        var value = langents.FirstOrDefault(e => e.GetString("Key").Equals(id) && e.PartitionKey.Equals(lang));

                        if (value != null)
                        {
                            values.Add(id, HttpUtility.UrlDecode(value.RowKey));
                        }
                    }

                    if (Compress)//compress
                    {
                        await blobClient.UploadAsync(BinaryData.FromBytes(CompressJSON(JsonConvert.SerializeObject(values))), overwrite: true);
                    }
                    else
                    {
                        await blobClient.UploadAsync(BinaryData.FromObjectAsJson(values), overwrite: true);
                    }
                }
            }
        }

        [FunctionName(nameof(GetMetaData))]
        public async static Task<MetaData> GetMetaData([ActivityTrigger] bool includeGroups)
        {
            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Langy'", select: new List<string> { "RowKey" });
            List<string> codes = new();

            AsyncPageable<TableEntity> queryResultsFilter2 = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Usage'");
            List<GroupObject> groups = new();

            // Iterate the <see cref="Pageable"> to access all queried entities.
            Task langTask = GetData(queryResultsFilter, codes);

            if (!includeGroups)
            {
                await langTask;

                return new MetaData()
                {
                    Groups = null,
                    Codes = codes,
                };
            }

            Task usageTask = GetData2(queryResultsFilter2, groups);

            await langTask;

            await usageTask;

            return new MetaData()
            {
                Groups = groups,
                Codes = codes,
            };

            static async Task GetData(AsyncPageable<TableEntity> queryResultsFilter, List<string> langs)
            {
                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    langs.Add(qEntity.RowKey);
                }
            }

            static async Task GetData2(AsyncPageable<TableEntity> queryResultsFilter, List<GroupObject> groups)
            {
                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    groups.Add(new GroupObject()
                    {
                        Group = qEntity.RowKey,
                        Keys = JsonConvert.DeserializeObject<List<string>>(qEntity.GetString("Usage"))
                    });
                }
            }
        }

        [FunctionName("Compile_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("Compile", null);

            log.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        public static byte[] CompressJSON(string originalString)
        {
            byte[] dataToCompress = Encoding.Unicode.GetBytes(originalString);

            using MemoryStream memoryStream = new();

            using (BrotliStream brotliStream = new(memoryStream, CompressionLevel.Optimal))
            {
                brotliStream.Write(dataToCompress, 0, dataToCompress.Length);
            }

            return memoryStream.ToArray();
        }
    }
}