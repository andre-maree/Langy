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
using System.Text.RegularExpressions;
using System.Linq;

namespace Langy
{
    public static class LangyAPI
    {
        [FunctionName(nameof(SaveGroupUsage))]
        public static async Task<HttpResponseMessage> SaveGroupUsage(
        [HttpTrigger(AuthorizationLevel.System, Route = "SaveGroupUsage")] HttpRequestMessage req)
        {
            GroupObject group = await req.Content.ReadAsAsync<GroupObject>();

            TableClient table = CreaTableClient();

            TableEntity tableEntity = new("Usage", HttpUtility.UrlEncodeUnicode(group.Group)) {
                {
                    "Usage",
                    JsonConvert.SerializeObject(group.Keys)
                }
            };

            await table.UpsertEntityAsync(tableEntity);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        public static TableClient CreaTableClient()
        {
            TableServiceClient serviceClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            TableClient table = serviceClient.GetTableClient("Langy");

            return table;
        }

        [FunctionName(nameof(SaveLanguage))]
        public static async Task<HttpResponseMessage> SaveLanguage(
        [HttpTrigger(AuthorizationLevel.System, Route = "SaveLanguage")] HttpRequestMessage req)
        {
            TableClient table = CreaTableClient();

            NewLanguage input = await req.Content.ReadAsAsync<NewLanguage>();

            Dictionary<string, string> keys = await GetLanguageItemsAsync(null);

            foreach (var kvp in input.LanguageItems)
            {
                if (!keys.ContainsKey(kvp.Key))
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The key '{kvp.Key}' was not specified in the language items list.")
                    };
                }
            }

            List<TableEntity> ents = new();

            foreach (KeyValuePair<string, string> kvp in input.LanguageItems)
            {
                var ent = ents.FirstOrDefault(e => e.RowKey.Equals(kvp.Key));

                TableEntity tableEntity = new()
                {
                    PartitionKey = input.Code
                };

                if (ent == null)
                {
                    if (kvp.Value.Length > 512)
                    {
                        tableEntity.Add("Text", kvp.Value);

                        tableEntity.RowKey = kvp.Value.Remove(512);
                    }
                    else
                    {
                        tableEntity.RowKey = kvp.Value;
                    }

                    tableEntity.Add("Key", kvp.Key);
                }
                else//duplicate
                {
                    if (kvp.Value.Length > 512)
                    {
                        tableEntity.Add("Text", kvp.Value);
                    }

                    tableEntity.Add("Duplicate", true);
                    tableEntity.RowKey = kvp.Key;
                }

                ents.Add(tableEntity);
            }

            TableEntity tableEntityM = new("Langy", input.Code)
            {
                {
                    "Language",
                    input.Name
                }
            };

            await table.UpsertEntityAsync(tableEntityM);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName(nameof(DeleteLanguage))]
        public static async Task<HttpResponseMessage> DeleteLanguage(
        [HttpTrigger(AuthorizationLevel.System, Route = "DeleteLanguage")] HttpRequestMessage req)
        {
            TableClient table = CreaTableClient();

            Dictionary<string, string> dyna = await req.Content.ReadAsAsync<Dictionary<string, string>>();

            await table.DeleteEntityAsync("Langy", dyna["Code"]);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName(nameof(SaveLanguageItem))]
        public static async Task<HttpResponseMessage> SaveLanguageItem(
        [HttpTrigger(AuthorizationLevel.System, Route = "SaveLanguageItem")] HttpRequestMessage req)
        {
            TableClient table = CreaTableClient();

            Task<Dictionary<string, string>> langtask = GetLanguagesAsync(table);

            string uid = Regex.Replace(Convert.ToBase64String(Guid.NewGuid().ToByteArray()), "[/+=]", "");

            Dictionary<string, string> data = await req.Content.ReadAsAsync<Dictionary<string, string>>();
            //List<string> fails = new();
            List<Task<(TableEntity, bool)>> tasks = new();
            List<Task<(TableEntity, bool)>> duptasks = new();

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
                bool islarge = false;

                if (rk.Length > 512)
                {
                    tableEntity.Add("Text", rk);

                    rk = rk.Remove(512);

                    islarge = true;
                }

                tableEntity.PartitionKey = kvp.Key;
                tableEntity.RowKey = rk;
                tableEntity.Add("Key", uid);

                if (tasks.Count < 1)
                {
                    tasks.Add(Insert(tableEntity, table));
                }
                else
                {
                    Task<(TableEntity, bool isDuplicate)> task = await Task.WhenAny(tasks);

                    if (task.Result.isDuplicate)
                    {
                        string rowkey = task.Result.Item1.RowKey;
                        task.Result.Item1.RowKey = task.Result.Item1.GetString("Key");
                        task.Result.Item1.Add("Duplicate", true);

                        if (islarge)
                        {
                            task.Result.Item1.Remove("Key");
                        }
                        else
                        {
                            task.Result.Item1["Key"] = rowkey;
                        }

                        duptasks.Add(Insert(task.Result.Item1, table));
                    }

                    tasks.Remove(task);

                    tasks.Add(Insert(tableEntity, table));
                }
            }

            while (tasks.Count > 0)
            {
                var task = await Task.WhenAny(tasks);

                if (task.Result.Item2)
                {
                    string rowkey = task.Result.Item1.RowKey;
                    task.Result.Item1.RowKey = task.Result.Item1.GetString("Key");
                    task.Result.Item1.Add("Duplicate", true);

                    if (task.Result.Item1.Keys.Contains("Text"))
                    {
                        task.Result.Item1.Remove("Key");
                    }
                    else
                    {
                        task.Result.Item1["Key"] = rowkey;
                    }

                    _ = tasks.Remove(task);

                    duptasks.Add(Insert(task.Result.Item1, table));
                }
                else
                {
                    tasks.Remove(task);
                }
            }

            await Task.WhenAll(duptasks);

            //if (fails.Any())
            //{
            //    return new HttpResponseMessage(HttpStatusCode.OK)
            //    {
            //        Content = new StringContent(JsonConvert.SerializeObject(new { Message = "The following items failed to save because they already exist:", Fails = fails }))
            //    };
            //}

            return new HttpResponseMessage(HttpStatusCode.OK);
        }

        private static async Task<(TableEntity, bool)> Insert(TableEntity tableEntity, TableClient tc)
        {
            try
            {
                Response res = await tc.AddEntityAsync(tableEntity);
            }
            catch (RequestFailedException ex)
            {
                if (ex.Status == 409)
                {
                    return (tableEntity, true);
                }
                else if (ex.Status == 404)
                {
                    await tc.CreateIfNotExistsAsync();

                    await Insert(tableEntity, tc);
                }
            }

            return (tableEntity, false);
        }

        [FunctionName(nameof(GetLanguageItem))]
        public static async Task<HttpResponseMessage> GetLanguageItem(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguageItem/{code}")] HttpRequestMessage req, string code)
        {
            TableClient table = CreaTableClient();

            string text = await req.Content.ReadAsStringAsync();

            Response<TableEntity> result = await table.GetEntityAsync<TableEntity>(code, HttpUtility.UrlEncode(text));

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(result.Value))
            };
        }

        [FunctionName(nameof(GetLanguageItems))]
        public static async Task<HttpResponseMessage> GetLanguageItems(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguageItems/{code?}")] HttpRequestMessage req, string code)
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(await GetLanguageItemsAsync(code)))
            };
        }

        public static async Task<Dictionary<string, string>> GetLanguageItemsAsync(string code)
        {
            TableClient table = CreaTableClient();

            Dictionary<string, string> data = new();

            if (code == null)
            {
                code = await GetTopOneLanguage(table);

                AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{code}'", select: new List<string> { "RowKey", "Key", "Duplicate" });

                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    if (qEntity.GetBoolean("Duplicate") != null)
                    {
                        data.Add(HttpUtility.UrlDecode(qEntity.RowKey), string.Empty);
                    }
                    else
                    {
                        data.Add(qEntity.GetString("Key"), string.Empty);
                    }
                }
            }
            else
            {
                AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{code}'", select: new List<string> { "RowKey", "Key", "Text", "Duplicate" });

                await foreach (TableEntity qEntity in queryResultsFilter)
                {
                    string text = qEntity.GetString("Text");

                    if (qEntity.GetBoolean("Duplicate") != null)
                    {
                        if (text != null)
                        {
                            data.Add(HttpUtility.UrlDecode(qEntity.RowKey), text);
                        }
                        else
                        {
                            data.Add(HttpUtility.UrlDecode(qEntity.RowKey), qEntity.GetString("Key"));
                        }
                    }
                    else
                    {
                        if (text != null)
                        {
                            data.Add(qEntity.GetString("Key"), text);
                        }
                        else
                        {
                            data.Add(qEntity.GetString("Key"), HttpUtility.UrlDecode(qEntity.RowKey));
                        }
                    }
                }
            }

            return data;
        }

        [FunctionName(nameof(GetLanguages))]
        public static async Task<HttpResponseMessage> GetLanguages(
                [HttpTrigger(AuthorizationLevel.Function, Route = "GetLanguages")] HttpRequest req)
        {
            TableClient table = CreaTableClient();

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