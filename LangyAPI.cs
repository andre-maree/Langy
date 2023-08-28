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

            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            table.CreateIfNotExists();

            TableEntity tableEntity = new("Usage", HttpUtility.UrlEncodeUnicode(group.Group)) {
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
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            table.CreateIfNotExists();

            Dictionary<string, string> dyna = await req.Content.ReadAsAsync<Dictionary<string, string>>();

            TableEntity tableEntity = new("Langy", dyna["Code"])
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
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
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
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            Task<Dictionary<string, string>> langtask = GetLanguagesAsync(table);

            string uid = Regex.Replace(Convert.ToBase64String(Guid.NewGuid().ToByteArray()), "[/+=]", "");

            Dictionary<string, string> data = await req.Content.ReadAsAsync<Dictionary<string, string>>();
            List<string> fails = new();
            List<Task<Response>> tasks = new();

            await langtask;

            Dictionary<string, string> langs = langtask.Result;

            if (langs.Count != data.Count)
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent($"The input count does not match the saved language code count.")
                };
            }

            foreach (KeyValuePair<string, string> lang in langs)
            {
                if (!data.ContainsKey(lang.Key))
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The input language code {lang.Key} is missing.")
                    };
                }
            }

            foreach (KeyValuePair<string, string> kvp in data)
            {
                string rk = HttpUtility.UrlEncodeUnicode(kvp.Value);
                TableEntity tableEntity = new();

                if (rk.Length > 512)
                {
                    tableEntity.Add("Text", rk);

                    rk = rk.Remove(512);
                }

                tableEntity.PartitionKey = kvp.Key;
                tableEntity.RowKey = rk;
                tableEntity.Add("Key", uid);

                //if (tasks.Count < 15)
                //{
                    tasks.Add(table.AddEntityAsync(tableEntity));
                //}
                //else
                //{
                    Task<Response> task = null;

                    try
                    {
                        task = await Task<Response>.WhenAny(tasks);

                    if(task.IsFaulted)
                    {
                        if (task.Exception.GetBaseException() is RequestFailedException rfex && rfex.Status == 409)
                        {
                            //task.Exception.

                            tableEntity.RowKey = uid;
                            tableEntity.Add("Duplicate", true);
                            tableEntity["Key"] = rk;

                            tasks.Remove(task); 
                            tasks.Add(table.AddEntityAsync(tableEntity));
                        }
                    }

                        tasks.Remove(task);

                        tasks.Add(table.AddEntityAsync(tableEntity));
                    }
                    catch (RequestFailedException ex)
                    {
                        if (ex.Status == 409)
                        {
                            tableEntity.RowKey = uid;
                            tableEntity.Add("Duplicate", true);
                            tableEntity["Key"] = rk;

                            task.Start();
                        }
                        else if (ex.Status == 404)
                        {
                            await table.CreateIfNotExistsAsync();

                            tasks.Remove(task);

                            tasks.Add(table.AddEntityAsync(tableEntity));
                        }
                    }
                }
            //}

            Task task2;

            try
            {
                task2 = await Task.WhenAny(tasks);
            }
            catch (RequestFailedException ex)
            {
                if (ex.Status == 409)
                {
                    //foreach (Task task in tasks.Where(t => !t.IsCompletedSuccessfully))
                    //{
                    //    fails.Add(task.Id.ToString());
                    //}
                }
                else if (ex.Status == 404)
                {
                    await table.CreateIfNotExistsAsync();

                    //List<Task> tt = tasks;

                    tasks.Clear();

                    //foreach (Task task in tt)
                    //{
                    //    task.Start();
                    //}
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
        public static async Task<HttpResponseMessage> GetLanguageItem(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguageItem/{code}")] HttpRequestMessage req, string code)
        {
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            string text = await req.Content.ReadAsStringAsync();

            Response<TableEntity> result = await table.GetEntityAsync<TableEntity>(code, HttpUtility.UrlEncode(text));

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(result.Value))
            };
        }

        [FunctionName("GetLanguageItems")]
        public static async Task<HttpResponseMessage> GetLanguageItems(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguageItems/{code?}")] HttpRequestMessage req, string code)
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(await GetLanguageItemsAsync(code)))
            };
        }

        private static async Task<Dictionary<string, string>> GetLanguageItemsAsync(string code)
        {
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");
            Dictionary<string, string> data = new();

            if (code == null)
            {
                code = await GetTopOneLanguage(table);

                AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{code}'", select: new List<string> { "Key" });
                
                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    data.Add(qEntity.GetString("Key"), string.Empty);
                }
            }
            else
            {
                AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{code}'", select: new List<string> { "RowKey", "Key", "Text" });

                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    data.Add(qEntity.GetString("Key"), qEntity.RowKey);
                }
            }

            return data;
        }

        [FunctionName("GetLanguages")]
        public static async Task<HttpResponseMessage> GetLanguages(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguages")] HttpRequest req)
        {
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            Dictionary<string, string> data = await GetLanguagesAsync(table);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(data))
            };
        }

        private static async Task<Dictionary<string, string>> GetLanguagesAsync(TableClient table)
        {
            Dictionary<string, string> data = new();

            AsyncPageable<TableEntity> queryResultsFilter2 = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Langy'", maxPerPage: 99999999);

            await foreach (TableEntity qEntity in queryResultsFilter2)
            {
                data.Add(qEntity.RowKey, qEntity.GetString("Language"));
            }

            return data;
        }

        private static async Task<string> GetTopOneLanguage(TableClient table)
        {
            AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Langy'", maxPerPage: 1);

            await foreach (TableEntity qEntity in queryResultsFilter)
            {
                return qEntity.RowKey;
            }

            return null;
        }
    }
}