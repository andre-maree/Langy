using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using System.Web;
using Azure.Data.Tables;
using Azure;
using System.Linq;
using System.Text.RegularExpressions;

namespace Langy
{
    public static class LangyAPI
    {
        [FunctionName("SaveGroupUsage")]
        public static async Task<HttpResponseMessage> SaveGroupUsage(
        [HttpTrigger(AuthorizationLevel.System, Route = "SaveGroupUsage")] HttpRequestMessage req)
        {
            GroupObject group = await req.Content.ReadAsAsync<GroupObject>();

            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            table.CreateIfNotExists();

            var tableEntity = new TableEntity("Usage", HttpUtility.UrlEncodeUnicode(group.Group)) {
                {
                    "Usage",
                    JsonConvert.SerializeObject(group.Keys)
                }
            };

            await table.UpsertEntityAsync(tableEntity);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName("SaveLanguage")]
        public static async Task<HttpResponseMessage> SaveLanguage(
        [HttpTrigger(AuthorizationLevel.System, Route = "SaveLanguage")] HttpRequestMessage req)
        {
            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            table.CreateIfNotExists();

            Dictionary<string, string> dyna = await req.Content.ReadAsAsync<Dictionary<string, string>>();

            var tableEntity = new TableEntity("Langy", dyna["Code"])
                {
                    {
                        "Language",
                        dyna["Language"]
                    }
                };

            await table.UpsertEntityAsync(tableEntity);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName("DeleteLanguage")]
        public static async Task<HttpResponseMessage> DeleteLanguage(
        [HttpTrigger(AuthorizationLevel.System, Route = "DeleteLanguage")] HttpRequestMessage req)
        {
            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            table.CreateIfNotExists();

            Dictionary<string, string> dyna = await req.Content.ReadAsAsync<Dictionary<string, string>>();

            await table.DeleteEntityAsync("Langy", dyna["Code"]);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName("SaveLanguageItem")]
        public static async Task<HttpResponseMessage> SaveLanguageItem(
        [HttpTrigger(AuthorizationLevel.System, Route = "SaveLanguageItem")] HttpRequestMessage req)
        {
            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            var langtask = GetLanguagesAsync(table);

            string uid = Regex.Replace(Convert.ToBase64String(Guid.NewGuid().ToByteArray()), "[/+=]", "");

            Dictionary<string, string> data = await req.Content.ReadAsAsync<Dictionary<string, string>>();
            List<string> fails = new();
            List<Task> tasks = new();

            await langtask;

            Dictionary<string, string> langs = langtask.Result;

            if (langs.Count != data.Count)
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent($"The input count does not match the saved language code counts.")
                };
            }

            foreach (var lang in langs)
            {
                if (!data.ContainsKey(lang.Key))
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The input language code {lang.Key} is missing.")
                    };
                }
            }

            foreach (var kvp in data)
            {
                string rk = HttpUtility.UrlEncodeUnicode(kvp.Value);
                var tableEntity = new TableEntity();

                if (rk.Length > 512)
                {
                    tableEntity.Add("Text", rk);

                    rk = rk.Remove(512);
                }

                tableEntity.PartitionKey = kvp.Key;
                tableEntity.RowKey = rk;
                tableEntity.Add("Key", uid);

                if (tasks.Count < 15)
                {
                    tasks.Add(table.AddEntityAsync(tableEntity));
                }
                else
                {
                    Task task = null;

                    try
                    {
                        task = await Task.WhenAny(tasks);

                        tasks.Remove(task);

                        tasks.Add(table.AddEntityAsync(tableEntity));
                    }
                    catch (RequestFailedException ex)
                    {
                        if (ex.Status == 409)
                        {
                            fails.Add(kvp.Key);

                            tasks.Remove(task);
                        }
                        else if (ex.Status == 404)
                        {
                            await table.CreateIfNotExistsAsync();

                            tasks.Remove(task);

                            tasks.Add(table.AddEntityAsync(tableEntity));
                        }
                    }
                }
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (RequestFailedException ex)
            {
                if (ex.Status == 409)
                {
                    foreach (var task in tasks.Where(t => !t.IsCompletedSuccessfully))
                    {
                        fails.Add(task.Id.ToString());
                    }
                }
                else if (ex.Status == 404)
                {
                    await table.CreateIfNotExistsAsync();

                    List<Task> tt = tasks;

                    tasks.Clear();

                    foreach (var task in tt)
                    {
                        task.Start();
                    }
                }
            }

            if (fails.Any())
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(JsonConvert.SerializeObject(new { Message = "The following items failed to save because they already exist:", Fails = fails }))
                };
            }

            return new HttpResponseMessage(HttpStatusCode.OK);
        }

        [FunctionName("GetLanguageItem")]
        public static async Task<HttpResponseMessage> CounterList(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguageItem/{code}")] HttpRequestMessage req, string code)
        {
            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            string text = await req.Content.ReadAsStringAsync();

            var result = await table.GetEntityAsync<TableEntity>(code, HttpUtility.UrlEncode(text));

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(result.Value))
            };
        }

        [FunctionName("GetLanguages")]
        public static async Task<HttpResponseMessage> GetLanguages(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguages")] HttpRequest req)
        {
            var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            Dictionary<string, string> data = await GetLanguagesAsync(table);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(data))
            };
        }

        private static async Task<Dictionary<string, string>> GetLanguagesAsync(TableClient table)
        {
            AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Langy'");
            Dictionary<string, string> data = new();

            await foreach (TableEntity qEntity in queryResultsFilter)
            {
                data.Add(qEntity.RowKey, qEntity.GetString("Language"));
            }

            return data;
        }
    }
}