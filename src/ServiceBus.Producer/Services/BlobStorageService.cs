using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace ServiceBus.Producer.Services
{
    public class BlobStorageService : IBlobStorageService
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _containerName;
        private readonly ILogger<BlobStorageService> _logger;

        public BlobStorageService(
            IConfiguration configuration,
            ILogger<BlobStorageService> logger)
        {
            _logger = logger;
            _containerName = configuration["BlobStorage:ContainerName"] ?? "masterdata-imports";

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

        public async Task<string> UploadFileAsync(string fileName, Stream fileStream, string contentType)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
                await containerClient.CreateIfNotExistsAsync();

                string blobName = $"{DateTime.UtcNow:yyyyMMdd}/{Guid.NewGuid():N}/{fileName}";
                var blobClient = containerClient.GetBlobClient(blobName);

                _logger.LogInformation("[BlobStorage] Uploading file {FileName} as {BlobName}...", fileName, blobName);

                await blobClient.UploadAsync(fileStream, new Azure.Storage.Blobs.Models.BlobUploadOptions
                {
                    HttpHeaders = new Azure.Storage.Blobs.Models.BlobHttpHeaders
                    {
                        ContentType = contentType
                    }
                });

                string blobUrl = blobClient.Uri.ToString();
                _logger.LogInformation("[BlobStorage] File uploaded successfully: {BlobUrl}", blobUrl);

                return blobUrl;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BlobStorage] Failed to upload file {FileName}", fileName);
                throw;
            }
        }

        public async Task<Stream?> DownloadFileAsync(string blobUrl)
        {
            try
            {
                var blobClient = new BlobClient(new Uri(blobUrl), new DefaultAzureCredential());
                var response = await blobClient.DownloadAsync();
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
