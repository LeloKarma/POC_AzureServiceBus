using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace ServiceBus.Consumer.Services
{
    public class BlobStorageService : IBlobStorageService
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ILogger<BlobStorageService> _logger;

        public BlobStorageService(
            IConfiguration configuration,
            ILogger<BlobStorageService> logger)
        {
            _logger = logger;

            string? connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING")
                ?? configuration["BlobStorage:ConnectionString"];

            if (!string.IsNullOrEmpty(connectionString))
            {
                _logger.LogInformation("[BlobStorage] Using connection string for Blob Storage.");
                _blobServiceClient = new BlobServiceClient(connectionString);
            }
            else
            {
                string accountName = configuration["BlobStorage:AccountName"] ?? "pocservicebustests";
                _logger.LogInformation($"[BlobStorage] Using DefaultAzureCredential for account: {accountName}.blob.core.windows.net");
                _blobServiceClient = new BlobServiceClient(new Uri($"https://{accountName}.blob.core.windows.net"), new DefaultAzureCredential());
            }
        }

        public async Task<Stream?> DownloadFileAsync(string blobUrl)
        {
            try
            {
                _logger.LogInformation("[BlobStorage] Downloading file from {BlobUrl}...", blobUrl);
                var blobClient = new BlobClient(new Uri(blobUrl), new DefaultAzureCredential());
                var response = await blobClient.DownloadAsync();
                _logger.LogInformation("[BlobStorage] File downloaded successfully from {BlobUrl}", blobUrl);
                return response.Value.Content;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BlobStorage] Failed to download file from {BlobUrl}", blobUrl);
                return null;
            }
        }
    }
}
