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
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Langy
{
    public static class LangyAPI
    {
        #region Save

        [FunctionName(nameof(SaveGroupUsage))]
        public static async Task<HttpResponseMessage> SaveGroupUsage(
        [HttpTrigger(AuthorizationLevel.System, "post", Route = "SaveGroupUsage")] HttpRequestMessage req)
        {
            GroupObject group = await req.Content.ReadAsAsync<GroupObject>();

            TableClient table = LangyHelper.CreaTableClient();

            Dictionary<string, string> existing = await GetLanguageItemsAsync(null, table);

            foreach (string key in group.Keys)
            {
                if (!existing.ContainsKey(key))
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The key '{key}' was not found in the existing keys list.")
                    };
                }
            }

            TableEntity tableEntity = new("Usage", HttpUtility.UrlEncode(group.Group)) {
                {
                    "Usage",
                    JsonConvert.SerializeObject(group.Keys)
                }
            };

            await table.UpsertEntityAsync(tableEntity);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName(nameof(SaveLanguage))]
        public static async Task<HttpResponseMessage> SaveLanguage(
        [HttpTrigger(AuthorizationLevel.System, "post", Route = "SaveLanguage")] HttpRequestMessage req)
        {
            TableClient table = LangyHelper.CreaTableClient();

            NewLanguage input = await req.Content.ReadAsAsync<NewLanguage>();

            input.Code = input.Code.ToLower();

            await table.CreateIfNotExistsAsync();

            Task<NullableResponse<TableEntity>> maincheck = table.GetEntityIfExistsAsync<TableEntity>("Langy", input.Code);

            Task<Dictionary<string, string>> existingtask = GetLanguageItemsAsync(null, table);
            
            await maincheck;
            
            if (maincheck.Result.HasValue)
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent($"This language already exists.")
                };
            }

            await existingtask;

            Dictionary<string, string> existing = existingtask.Result;

            bool is1st = true;

            if (existing.Count > 0)
            {
                if (existing.Count != input.LanguageItems.Count)
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The key count is incorrect: input = {input.LanguageItems.Count} and existing = {existing.Count}.")
                    };
                }

                foreach (LanguageItem kvp in input.LanguageItems)
                {
                    if (string.IsNullOrWhiteSpace(kvp.Key))
                    {
                        return new HttpResponseMessage(HttpStatusCode.BadRequest)
                        {
                            Content = new StringContent($"A key was found that does not contain a value.")
                        };
                    }

                    if (!existing.ContainsKey(kvp.Key))
                    {
                        return new HttpResponseMessage(HttpStatusCode.BadRequest)
                        {
                            Content = new StringContent($"The key '{kvp.Key}' was not found in the the existing keys.")
                        };
                    }

                    if (input.LanguageItems.Where(e => e.Key.Equals(kvp.Key)).Take(2).Count() == 2)
                    {
                        return new HttpResponseMessage(HttpStatusCode.BadRequest)
                        {
                            Content = new StringContent($"The key '{kvp.Key}' is a duplicate.")
                        };
                    }
                }

                is1st = false;
            }

            List<TableTransactionAction> bop = new();

            foreach (LanguageItem kvp in input.LanguageItems)
            {
                string uid = is1st
                    ? Regex.Replace(Convert.ToBase64String(Guid.NewGuid().ToByteArray()), "[/+=]", "")
                    : kvp.Key;

                TableTransactionAction ent = bop.FirstOrDefault(e => e.Entity.RowKey.Equals(kvp.Text));

                TableEntity tableEntity = new()
                {
                    PartitionKey = input.Code
                };

                if (ent == null)
                {
                    if (kvp.Text.Length > 512)
                    {
                        tableEntity.Add("Text", kvp.Text);

                        tableEntity.RowKey = HttpUtility.UrlEncode(kvp.Text.Remove(512));
                    }
                    else
                    {
                        tableEntity.RowKey = HttpUtility.UrlEncode(kvp.Text);
                    }

                    tableEntity.Add("Key", uid);
                }
                else//duplicate
                {
                    if (kvp.Text.Length > 512)
                    {
                        tableEntity.Add("Text", kvp.Text);
                    }
                    else
                    {
                        tableEntity.Add("Key", kvp.Text);
                    }

                    tableEntity.Add("Duplicate", true);
                    tableEntity.RowKey = uid;
                }

                bop.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, tableEntity));

                if (bop.Count == 100)
                {
                    await table.SubmitTransactionAsync(bop);

                    bop.Clear();
                }
            }

            if (bop.Any())
            {
                await table.SubmitTransactionAsync(bop);
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

        [FunctionName(nameof(AddItemsToUsageGroup))]
        public static async Task<HttpResponseMessage> AddItemsToUsageGroup(
        [HttpTrigger(AuthorizationLevel.System, "post", Route = "AddItemsToUsageGroup")] HttpRequestMessage req)
        {
            TableClient table = LangyHelper.CreaTableClient();

            GroupObject group = await req.Content.ReadAsAsync<GroupObject>();

            Dictionary<string, string> existing = await GetLanguageItemsAsync(null, table);

            foreach (string key in group.Keys)
            {
                if (!existing.ContainsKey(key))
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The key '{key}' was not found in the existing keys list.")
                    };
                }
            }

            TableEntity tableEntity = await table.GetEntityAsync<TableEntity>("Usage", group.Group);

            List<string> items = JsonConvert.DeserializeObject<List<string>>(tableEntity.GetString("Usage"));

            items.AddRange(group.Keys);

            if (items.Count != items.Distinct().Count())
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent($"Can not add a duplicate key to the usage group.")
                };
            }

            tableEntity["Usage"] = JsonConvert.SerializeObject(items);

            await table.UpsertEntityAsync(tableEntity);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName(nameof(SaveLanguageItem))]
        public static async Task<HttpResponseMessage> SaveLanguageItem(
        [HttpTrigger(AuthorizationLevel.System, "post", Route = "SaveLanguageItem")] HttpRequestMessage req)
        {
            TableClient table = LangyHelper.CreaTableClient();

            Task<Dictionary<string, string>> langtask = GetLanguagesAsync(table);

            string uid = Regex.Replace(Convert.ToBase64String(Guid.NewGuid().ToByteArray()), "[/+=]", "");

            Dictionary<string, string> data = await req.Content.ReadAsAsync<Dictionary<string, string>>();

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
                        Content = new StringContent($"The input language code '{lang.Key}' is missing.")
                    };
                }
            }

            foreach (KeyValuePair<string, string> kvp in data)
            {
                string rk = HttpUtility.UrlEncode(kvp.Value);

                TableEntity tableEntity = new();
                bool islarge = false;

                if (rk.Length > 512)
                {
                    tableEntity.Add("Text", kvp.Value);

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
                            task.Result.Item1["Key"] = HttpUtility.UrlDecode(rowkey);
                        }

                        duptasks.Add(Insert(task.Result.Item1, table));
                    }

                    tasks.Remove(task);

                    tasks.Add(Insert(tableEntity, table));
                }
            }

            while (tasks.Count > 0)
            {
                Task<(TableEntity, bool)> task = await Task.WhenAny(tasks);

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
                        task.Result.Item1["Key"] = HttpUtility.UrlDecode(rowkey);
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

        #endregion

        #region Get

        [FunctionName(nameof(GetLanguageItem))]
        public static async Task<HttpResponseMessage> GetLanguageItem(
                [HttpTrigger(AuthorizationLevel.Function, "post", Route = "GetLanguageItem/{code}")] HttpRequestMessage req, string code)
        {
            TableClient table = LangyHelper.CreaTableClient();

            string text = await req.Content.ReadAsStringAsync();

            Response<TableEntity> result = await table.GetEntityAsync<TableEntity>(code, HttpUtility.UrlEncode(text));

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(result.Value))
            };
        }

        [FunctionName(nameof(GetLanguageItems))]
        public static async Task<HttpResponseMessage> GetLanguageItems(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "GetLanguageItems/{code?}")] HttpRequestMessage req,
            string code)
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonConvert.SerializeObject(await GetLanguageItemsAsync(code, LangyHelper.CreaTableClient())))
            };
        }

        [FunctionName(nameof(GetTranslations))]
        public static async Task<Dictionary<string, Dictionary<string, string>>> GetTranslations(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "GetTranslations/{code}")] HttpRequestMessage req, string code)
        {
            BlobContainerClient containerClient = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "langy-translations");

            List<string> groups = await req.Content.ReadAsAsync<List<string>>();

            Dictionary<string, Dictionary<string, string>> data = new();

            foreach (string group in groups)
            {
                BlobClient blobClient = containerClient.GetBlobClient(code + "-" + group);
                BlobDownloadResult downloadResult = await blobClient.DownloadContentAsync();
                Dictionary<string, string> blobContents = JsonConvert.DeserializeObject<Dictionary<string, string>>(downloadResult.Content.ToString());
                data.Add(group, blobContents);
            }

            return data;
        }

        [FunctionName(nameof(GetUsageGroups))]
        public static async Task<List<GroupObject>> GetUsageGroups(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "GetUsageGroups/{group?}")] HttpRequestMessage req, string group)
        {
            TableClient table = LangyHelper.CreaTableClient();

            List<GroupObject> data = new();

            group = group == null ? "" : $"and RowKey eq '{group}'";

            AsyncPageable<TableEntity> queryResultsFilter = table.QueryAsync<TableEntity>(filter: $"PartitionKey eq 'Usage' {group}", select: new List<string> { "RowKey", "Usage" });

            await foreach (TableEntity qEntity in queryResultsFilter)
            {
                data.Add(new GroupObject()
                {
                    Group = qEntity.RowKey,
                    Keys = JsonConvert.DeserializeObject<List<string>>(qEntity.GetString("Usage"))
                });
            }

            return data;
        }

        public static async Task<Dictionary<string, string>> GetLanguageItemsAsync(string code, TableClient table)
        {
            //TableClient table = LangyHelper.CreaTableClient();

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
                            data.Add(qEntity.RowKey, qEntity.GetString("Key"));
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
                [HttpTrigger(AuthorizationLevel.Function, "get", Route = "GetLanguages")] HttpRequest req)
        {
            TableClient table = LangyHelper.CreaTableClient();

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

        #endregion

        #region Delete

        [FunctionName(nameof(RemoveItemsFromUsageGroup))]
        public static async Task<HttpResponseMessage> RemoveItemsFromUsageGroup(
        [HttpTrigger(AuthorizationLevel.System, "delete", Route = "RemoveItemsFromUsageGroup")] HttpRequestMessage req)
        {
            TableClient table = LangyHelper.CreaTableClient();

            GroupObject group = await req.Content.ReadAsAsync<GroupObject>();

            Dictionary<string, string> existing = await GetLanguageItemsAsync(null, table);

            foreach (string key in group.Keys)
            {
                if (!existing.ContainsKey(key))
                {
                    return new HttpResponseMessage(HttpStatusCode.BadRequest)
                    {
                        Content = new StringContent($"The key '{key}' was not found in the existing keys list.")
                    };
                }
            }

            TableEntity tableEntity = await table.GetEntityAsync<TableEntity>("Usage", group.Group);

            List<string> items = JsonConvert.DeserializeObject<List<string>>(tableEntity.GetString("Usage"));

            foreach (string key in group.Keys)
            {
                items.Remove(key);
            }

            tableEntity["Usage"] = JsonConvert.SerializeObject(items);

            await table.UpsertEntityAsync(tableEntity);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        [FunctionName(nameof(DeleteLanguage))]
        public static async Task<HttpResponseMessage> DeleteLanguage(
        [HttpTrigger(AuthorizationLevel.System, "delete", Route = "DeleteLanguage")] HttpRequestMessage req)
        {
            TableClient table = LangyHelper.CreaTableClient();

            Dictionary<string, string> dyna = await req.Content.ReadAsAsync<Dictionary<string, string>>();

            await table.DeleteEntityAsync("Langy", dyna["Code"]);

            return req.CreateResponse(HttpStatusCode.OK);
        }

        #endregion
    }
}