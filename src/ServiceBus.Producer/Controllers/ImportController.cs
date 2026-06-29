using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using ServiceBus.Contracts.Commands;
using ServiceBus.Producer.Services;

namespace ServiceBus.Producer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ImportController : ControllerBase
    {
        private readonly IServiceBusPublisher _publisher;
        private readonly ServiceBusClient _client;
        private readonly string _queueName;
        private readonly IBlobStorageService _blobStorageService;

        public ImportController(
            IServiceBusPublisher publisher,
            ServiceBusClient client,
            IConfiguration configuration,
            IBlobStorageService blobStorageService)
        {
            _publisher = publisher;
            _client = client;
            _queueName = configuration["ServiceBus:QueueName"] ?? "masterdata-import-queue";
            _blobStorageService = blobStorageService;
        }

        public record ImportRequest(string ImportType, string FileName, string? BlobUrl, long FileSizeBytes, int UserId);

        /// <summary>
        /// Upload a file and trigger import via Service Bus Queue.
        /// </summary>
        [HttpPost("upload")]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> UploadAndImport([FromQuery] string importType, [FromQuery] int userId, IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                return BadRequest("No file uploaded.");
            }

            try
            {
                string? blobUrl = null;

                // Try to upload to blob storage if available
                try
                {
                    blobUrl = await _blobStorageService.UploadFileAsync(file.FileName, file.OpenReadStream(), file.ContentType);
                }
                catch (Exception blobEx)
                {
                    // If blob storage fails, continue without it (fallback mode)
                    Console.WriteLine($"[Warning] Blob storage upload failed: {blobEx.Message}. Continuing without blob storage.");
                }

                // Create and send command
                var command = new ImportMasterDataCommand
                {
                    ImportType = importType,
                    FileName = file.FileName,
                    BlobReference = blobUrl, // Will be null if blob storage is not available
                    FileSizeBytes = file.Length,
                    RequestedBy = userId
                };

                await _publisher.SendCommandAsync(command);

                return Accepted(new
                {
                    CommandId = command.CommandId,
                    Status = "Pending",
                    Message = blobUrl != null 
                        ? "File uploaded to blob storage and import command enqueued." 
                        : "Import command enqueued (blob storage not available, using simulation mode).",
                    BlobUrl = blobUrl,
                    FileSize = file.Length
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = $"Failed to process upload: {ex.Message}" });
            }
        }

        /// <summary>
        /// Trigger a normal asynchronous import via Service Bus Queue.
        /// </summary>
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> TriggerImport([FromBody] ImportRequest request)
        {
            var command = new ImportMasterDataCommand
            {
                ImportType = request.ImportType,
                FileName = request.FileName,
                BlobReference = request.BlobUrl, // Optional for testing
                FileSizeBytes = request.FileSizeBytes,
                RequestedBy = request.UserId
            };

            await _publisher.SendCommandAsync(command);

            return Accepted(new { CommandId = command.CommandId, Status = "Pending", Message = "Import command successfully enqueued." });
        }

        /// <summary>
        /// Trigger an import command designed to fail and go to DLQ after 3 retries.
        /// </summary>
        [HttpPost("test-dlq")]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> TriggerDlqTest()
        {
            var command = new ImportMasterDataCommand
            {
                ImportType = "FAIL_TRIGGER",
                FileName = "toxic_data_v1.xlsx",
                BlobReference = null, // Not needed for DLQ test
                FileSizeBytes = 1024,
                RequestedBy = 999
            };

            await _publisher.SendCommandAsync(command);

            return Accepted(new
            {
                CommandId = command.CommandId,
                Status = "Pending",
                Message = "Fail-trigger command enqueued. It will fail 3 times in the worker and end up in the DLQ."
            });
        }

        /// <summary>
        /// Trigger a batch of imports to demonstrate Load Leveling.
        /// </summary>
        [HttpPost("batch")]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> TriggerBatch([FromQuery] int count = 10)
        {
            var commandIds = new List<string>();

            for (int i = 1; i <= count; i++)
            {
                var command = new ImportMasterDataCommand
                {
                    ImportType = i % 2 == 0 ? "Country" : "Port",
                    FileName = $"batch_file_{i}.xlsx",
                    BlobReference = null, // Not needed for batch test
                    FileSizeBytes = 5000 + (i * 100),
                    RequestedBy = 100 + i
                };

                await _publisher.SendCommandAsync(command);
                commandIds.Add(command.CommandId);
            }

            return Accepted(new
            {
                Message = $"Enqueued batch of {count} import commands to demonstrate load leveling.",
                CommandIds = commandIds
            });
        }

        /// <summary>
        /// Inspect (Peek) messages currently in the Dead-Letter Queue (DLQ).
        /// </summary>
        [HttpGet("dlq")]
        public async Task<IActionResult> PeekDlq()
        {
            string dlqPath = $"{_queueName}/$DeadLetterQueue";
            await using var receiver = _client.CreateReceiver(dlqPath);

            try
            {
                IReadOnlyList<ServiceBusReceivedMessage> dlqMessages = await receiver.PeekMessagesAsync(maxMessages: 10);
                var resultList = new List<object>();

                foreach (var msg in dlqMessages)
                {
                    // Extract dead letter properties
                    msg.ApplicationProperties.TryGetValue("DeadLetterReason", out object? reason);
                    msg.ApplicationProperties.TryGetValue("DeadLetterErrorDescription", out object? description);

                    resultList.Add(new
                    {
                        MessageId = msg.MessageId,
                        Subject = msg.Subject,
                        DeadLetterReason = reason?.ToString(),
                        DeadLetterErrorDescription = description?.ToString(),
                        EnqueuedTime = msg.EnqueuedTime,
                        Body = msg.Body.ToString()
                    });
                }

                return Ok(new
                {
                    DlqQueuePath = dlqPath,
                    MessageCount = resultList.Count,
                    Messages = resultList
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = $"Failed to peek DLQ: {ex.Message}" });
            }
        }
    }
}
