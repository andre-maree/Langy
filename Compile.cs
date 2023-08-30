using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using System.Text;

namespace Langy
{
    public class Compile
    {
        private static readonly bool Compress = false;

        [FunctionName("Compile")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            List<string> outputs = new();

            MetaData metas = await context.CallActivityAsync<MetaData>(nameof(GetMetaData), true);

            outputs.AddRange(metas.Codes);

            await context.CallActivityAsync(nameof(Process), metas);

            return "Compilation complete.";
        }

        [FunctionName(nameof(Process))]
        public static async Task Process([ActivityTrigger] MetaData metas)
        {
            List<Task> blobtasks = new();

            foreach (string lang in metas.Codes)
            {
                if (blobtasks.Count < 15)
                {
                    blobtasks.Add(ProcessCode(metas, lang));
                }
                else
                {
                    Task t = await Task.WhenAny(blobtasks);

                    blobtasks.Remove(t);

                    blobtasks.Add(ProcessCode(metas, lang));
                }
            }

            await Task.WhenAll(blobtasks);
        }

        private static async Task ProcessCode(MetaData metas, string lang)
        {
            BlobContainerClient containerClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "langy-translations");

            Dictionary<string, string> langitems = await LangyAPI.GetLanguageItemsAsync(lang);

            foreach (GroupObject group in metas.Groups)
            {
                Dictionary<string, string> values = new();

                BlobClient blobClient = containerClient.GetBlobClient(lang + "-" + group.Group);

                foreach (string id in group.Keys)
                {
                    string value = langitems[id];

                    if (value != null)
                    {
                        values.Add(id, value);
                    }
                }

                try
                {
                    if (Compress)//compress
                    {
                        await blobClient.UploadAsync(BinaryData.FromBytes(CompressJSON(JsonConvert.SerializeObject(values))), overwrite: true);
                    }
                    else
                    {
                        await blobClient.UploadAsync(BinaryData.FromObjectAsJson(values), overwrite: true);
                    }
                }
                catch (RequestFailedException ex)
                {
                    if (ex.Status == 404)
                    {
                        await containerClient.CreateIfNotExistsAsync();

                        await ProcessCode(metas, lang);
                    }
                }
            }
        }

        [FunctionName(nameof(GetMetaData))]
        public async static Task<MetaData> GetMetaData([ActivityTrigger] bool includeGroups)
        {
            TableClient table = LangyHelper.CreaTableClient();

            AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Langy'", select: new List<string> { "RowKey" });
            List<string> codes = new();

            AsyncPageable<TableEntity> queryResultsFilter2 = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Usage'");
            List<GroupObject> groups = new();

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

        [FunctionName(nameof(Compile_Start))]
        public static async Task<HttpResponseMessage> Compile_Start(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Compile_HttpStart/{waitseconds:int?}")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            int? waitseconds)
        {
            string instanceId = await starter.StartNewAsync("Compile", null);

            if (waitseconds.HasValue && waitseconds.Value > 0)
            {
                return await starter.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, timeout: TimeSpan.FromSeconds(waitseconds.Value));
            }

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        private static byte[] CompressJSON(string originalString)
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