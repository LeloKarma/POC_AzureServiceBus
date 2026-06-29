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
        private readonly ILogger<ImportController> _logger;
        private static readonly string[] ValidImportTypes = { "Country", "Port", "Vessel", "FAIL_TRIGGER" };
        private const long MaxFileSizeBytes = 50 * 1024 * 1024; // 50MB

        public ImportController(
            IServiceBusPublisher publisher,
            ServiceBusClient client,
            IConfiguration configuration,
            ILogger<ImportController> logger)
        {
            _publisher = publisher;
            _client = client;
            _queueName = configuration["ServiceBus:QueueName"] ?? "masterdata-import-queue";
            _logger = logger;
        }

        public record ImportRequest(string ImportType, string FileName, long FileSizeBytes, int UserId);

        private IActionResult ValidateImportRequest(string importType, long fileSize)
        {
            if (string.IsNullOrWhiteSpace(importType))
            {
                _logger.LogWarning("[Validation] Import type is empty");
                return BadRequest("Import type is required.");
            }

            if (!ValidImportTypes.Contains(importType, StringComparer.OrdinalIgnoreCase))
            {
                _logger.LogWarning("[Validation] Invalid import type: {ImportType}. Valid types: {ValidTypes}", 
                    importType, string.Join(", ", ValidImportTypes));
                return BadRequest($"Invalid import type '{importType}'. Valid types: {string.Join(", ", ValidImportTypes)}");
            }

            if (fileSize <= 0)
            {
                _logger.LogWarning("[Validation] File size must be positive: {FileSize}", fileSize);
                return BadRequest("File size must be greater than 0.");
            }

            // Custom validation rules per import type
            var (minSize, maxSize) = importType.ToUpperInvariant() switch
            {
                "COUNTRY" => (100, 10 * 1024 * 1024), // 100 bytes to 10MB
                "PORT" => (500, 20 * 1024 * 1024),    // 500 bytes to 20MB
                "VESSEL" => (1000, 50 * 1024 * 1024), // 1KB to 50MB
                "FAIL_TRIGGER" => (0, long.MaxValue), // No restrictions for testing
                _ => (0, MaxFileSizeBytes)
            };

            if (fileSize < minSize)
            {
                _logger.LogWarning("[Validation] File size too small for {ImportType}: {FileSize} < {MinSize}", 
                    importType, fileSize, minSize);
                return BadRequest($"File size for {importType} imports must be at least {minSize} bytes.");
            }

            if (fileSize > maxSize)
            {
                _logger.LogWarning("[Validation] File size too large for {ImportType}: {FileSize} > {MaxSize}", 
                    importType, fileSize, maxSize);
                return BadRequest($"File size for {importType} imports cannot exceed {maxSize / (1024 * 1024)}MB.");
            }

            return null!;
        }

        /// <summary>
        /// Upload multiple files and trigger imports via Service Bus Queue.
        /// </summary>
        [HttpPost("upload-multiple")]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> UploadMultiple([FromQuery] string importType, [FromQuery] int userId, IFormFileCollection files)
        {
            _logger.LogInformation("[UploadMultiple] Starting batch upload for import type: {ImportType}, user: {UserId}, file count: {FileCount}", 
                importType, userId, files?.Count ?? 0);

            if (files == null || files.Count == 0)
            {
                _logger.LogWarning("[UploadMultiple] No files uploaded");
                return BadRequest("No files uploaded.");
            }

            var validationError = ValidateImportRequest(importType, files.Sum(f => f.Length));
            if (validationError != null)
            {
                return validationError;
            }

            var results = new List<object>();

            foreach (var file in files)
            {
                try
                {
                    _logger.LogInformation("[UploadMultiple] Processing file: {FileName}, size: {FileSize}", file.FileName, file.Length);

                    var command = new ImportMasterDataCommand
                    {
                        ImportType = importType,
                        FileName = file.FileName,
                        BlobReference = null,
                        FileSizeBytes = file.Length,
                        RequestedBy = userId
                    };

                    await _publisher.SendCommandAsync(command);

                    results.Add(new
                    {
                        FileName = file.FileName,
                        CommandId = command.CommandId,
                        Status = "Pending",
                        FileSize = file.Length
                    });

                    _logger.LogInformation("[UploadMultiple] Successfully enqueued file: {FileName}, CommandId: {CommandId}", 
                        file.FileName, command.CommandId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[UploadMultiple] Failed to process file: {FileName}", file.FileName);
                    results.Add(new
                    {
                        FileName = file.FileName,
                        Error = ex.Message
                    });
                }
            }

            _logger.LogInformation("[UploadMultiple] Batch upload completed. Total: {Total}, Successful: {Successful}, Failed: {Failed}", 
                results.Count, results.Count(r => ((dynamic)r).Error == null), results.Count(r => ((dynamic)r).Error != null));

            return Accepted(new
            {
                Message = $"Enqueued {results.Count} import commands.",
                Results = results
            });
        }

        /// <summary>
        /// Upload a single file and trigger import via Service Bus Queue.
        /// </summary>
        [HttpPost("upload")]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> UploadAndImport([FromQuery] string importType, [FromQuery] int userId, IFormFile file)
        {
            _logger.LogInformation("[Upload] Starting single file upload for import type: {ImportType}, user: {UserId}, file: {FileName}", 
                importType, userId, file?.FileName);

            if (file == null || file.Length == 0)
            {
                _logger.LogWarning("[Upload] No file uploaded");
                return BadRequest("No file uploaded.");
            }

            var validationError = ValidateImportRequest(importType, file.Length);
            if (validationError != null)
            {
                return validationError;
            }

            try
            {
                var command = new ImportMasterDataCommand
                {
                    ImportType = importType,
                    FileName = file.FileName,
                    BlobReference = null,
                    FileSizeBytes = file.Length,
                    RequestedBy = userId
                };

                await _publisher.SendCommandAsync(command);

                _logger.LogInformation("[Upload] Successfully enqueued file: {FileName}, CommandId: {CommandId}", 
                    file.FileName, command.CommandId);

                return Accepted(new
                {
                    CommandId = command.CommandId,
                    Status = "Pending",
                    Message = "Import command enqueued.",
                    FileName = file.FileName,
                    FileSize = file.Length
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Upload] Failed to process file: {FileName}", file.FileName);
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
            _logger.LogInformation("[TriggerImport] Starting import for type: {ImportType}, file: {FileName}, user: {UserId}", 
                request.ImportType, request.FileName, request.UserId);

            var validationError = ValidateImportRequest(request.ImportType, request.FileSizeBytes);
            if (validationError != null)
            {
                return validationError;
            }

            var command = new ImportMasterDataCommand
            {
                ImportType = request.ImportType,
                FileName = request.FileName,
                BlobReference = null,
                FileSizeBytes = request.FileSizeBytes,
                RequestedBy = request.UserId
            };

            await _publisher.SendCommandAsync(command);

            _logger.LogInformation("[TriggerImport] Successfully enqueued command: {CommandId}", command.CommandId);

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
            _logger.LogInformation("[Batch] Starting batch import of {Count} commands", count);

            var commandIds = new List<string>();

            for (int i = 1; i <= count; i++)
            {
                var command = new ImportMasterDataCommand
                {
                    ImportType = i % 2 == 0 ? "Country" : "Port",
                    FileName = $"batch_file_{i}.xlsx",
                    BlobReference = null,
                    FileSizeBytes = 5000 + (i * 100),
                    RequestedBy = 100 + i
                };

                await _publisher.SendCommandAsync(command);
                commandIds.Add(command.CommandId);
            }

            _logger.LogInformation("[Batch] Successfully enqueued {Count} commands", count);

            return Accepted(new
            {
                Message = $"Enqueued batch of {count} import commands to demonstrate load leveling.",
                CommandIds = commandIds
            });
        }

        /// <summary>
        /// Bulk operation - Send multiple commands in a single message for efficient processing.
        /// </summary>
        [HttpPost("bulk")]
        [ProducesResponseType(StatusCodes.Status202Accepted)]
        public async Task<IActionResult> TriggerBulk([FromBody] List<ImportRequest> requests)
        {
            _logger.LogInformation("[Bulk] Starting bulk operation with {Count} requests", requests?.Count ?? 0);

            if (requests == null || requests.Count == 0)
            {
                _logger.LogWarning("[Bulk] No requests provided");
                return BadRequest("No requests provided.");
            }

            if (requests.Count > 100)
            {
                _logger.LogWarning("[Bulk] Too many requests: {Count} > 100", requests.Count);
                return BadRequest("Maximum 100 requests per bulk operation.");
            }

            var results = new List<object>();

            foreach (var request in requests)
            {
                var validationError = ValidateImportRequest(request.ImportType, request.FileSizeBytes);
                if (validationError != null)
                {
                    results.Add(new
                    {
                        FileName = request.FileName,
                        Error = "Validation failed"
                    });
                    continue;
                }

                try
                {
                    var command = new ImportMasterDataCommand
                    {
                        ImportType = request.ImportType,
                        FileName = request.FileName,
                        BlobReference = null,
                        FileSizeBytes = request.FileSizeBytes,
                        RequestedBy = request.UserId
                    };

                    await _publisher.SendCommandAsync(command);

                    results.Add(new
                    {
                        FileName = request.FileName,
                        CommandId = command.CommandId,
                        Status = "Pending"
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[Bulk] Failed to enqueue request for file: {FileName}", request.FileName);
                    results.Add(new
                    {
                        FileName = request.FileName,
                        Error = ex.Message
                    });
                }
            }

            _logger.LogInformation("[Bulk] Bulk operation completed. Total: {Total}, Successful: {Successful}, Failed: {Failed}", 
                results.Count, results.Count(r => ((dynamic)r).Error == null), results.Count(r => ((dynamic)r).Error != null));

            return Accepted(new
            {
                Message = $"Bulk operation processed {results.Count} requests.",
                Results = results
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
