using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using OfficeOpenXml;
using ServiceBus.Contracts.Commands;
using ServiceBus.Contracts.Events;

namespace ServiceBus.Consumer.Services
{
    public class ImportProcessor : IImportProcessor
    {
        private readonly ILogger<ImportProcessor> _logger;
        private readonly IBlobStorageService _blobStorageService;

        public ImportProcessor(
            ILogger<ImportProcessor> logger,
            IBlobStorageService blobStorageService)
        {
            _logger = logger;
            _blobStorageService = blobStorageService;
            
            // Set EPPlus license context for non-commercial use
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        }

        public async Task<ImportCompletedEvent> ProcessImportAsync(ImportMasterDataCommand command, CancellationToken cancellationToken)
        {
            var stopwatch = Stopwatch.StartNew();
            _logger.LogInformation("[Processor] Starting processing for Job/Command: {CommandId}, Type: {ImportType}, File: {FileName}",
                command.CommandId, command.ImportType, command.FileName);

            // 1. Simulate Fail Trigger for DLQ testing
            if (string.Equals(command.ImportType, "FAIL_TRIGGER", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("[Processor] Simulating processing failure for {CommandId} (FAIL_TRIGGER detected).", command.CommandId);
                throw new InvalidOperationException("Toxic payload format exception. Failed to parse row 127: invalid column layout.");
            }

            // 2. Download file from blob storage if blob reference is provided
            Stream? fileStream = null;
            int totalRows = 0;
            int errorCount = 0;
            string? errorMessage = null;

            try
            {
                if (!string.IsNullOrEmpty(command.BlobReference))
                {
                    _logger.LogInformation("[Processor] Downloading file from blob storage: {BlobUrl}", command.BlobReference);
                    fileStream = await _blobStorageService.DownloadFileAsync(command.BlobReference);
                    
                    if (fileStream == null)
                    {
                        throw new InvalidOperationException("Failed to download file from blob storage.");
                    }

                    // 3. Parse Excel file using EPPlus
                    _logger.LogInformation("[Processor] Parsing Excel file: {FileName}", command.FileName);
                    
                    using var package = new ExcelPackage(fileStream);
                    var worksheet = package.Workbook.Worksheets[0]; // First worksheet
                    
                    totalRows = worksheet.Dimension?.Rows ?? 0;
                    _logger.LogInformation("[Processor] Found {RowCount} rows in Excel file", totalRows);

                    // 4. Validate and process rows based on import type
                    if (totalRows > 0)
                    {
                        await ProcessRowsByImportType(worksheet, command.ImportType, cancellationToken);
                    }
                }
                else
                {
                    // Fallback to simulation if no blob reference
                    _logger.LogWarning("[Processor] No blob reference provided, using simulation mode.");
                    await Task.Delay(1500, cancellationToken);
                    totalRows = command.FileSizeBytes > 0 ? (int)(command.FileSizeBytes / 50) : 100;
                }

                stopwatch.Stop();
                _logger.LogInformation("[Processor] Successfully finished processing job: {CommandId} in {ElapsedMs}ms.", 
                    command.CommandId, stopwatch.ElapsedMilliseconds);

                return new ImportCompletedEvent
                {
                    CommandId = command.CommandId,
                    ImportType = command.ImportType,
                    Status = "Completed",
                    TotalRowsProcessed = totalRows,
                    ErrorCount = errorCount,
                    ErrorMessage = errorMessage,
                    CompletedAt = DateTime.UtcNow,
                    Duration = stopwatch.Elapsed
                };
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _logger.LogError(ex, "[Processor] Failed to process job: {CommandId}", command.CommandId);
                
                return new ImportCompletedEvent
                {
                    CommandId = command.CommandId,
                    ImportType = command.ImportType,
                    Status = "Failed",
                    TotalRowsProcessed = totalRows,
                    ErrorCount = 1,
                    ErrorMessage = ex.Message,
                    CompletedAt = DateTime.UtcNow,
                    Duration = stopwatch.Elapsed
                };
            }
            finally
            {
                fileStream?.Dispose();
            }
        }

        private async Task ProcessRowsByImportType(ExcelWorksheet worksheet, string importType, CancellationToken cancellationToken)
        {
            // Simulate different processing times based on import type
            int delayMs = importType switch
            {
                "Country" => 1000,
                "Port" => 2000,
                "Vessel" => 3000,
                _ => 1500
            };

            await Task.Delay(delayMs, cancellationToken);

            // Here you would add actual validation and database saving logic
            // For now, we just log the processing
            _logger.LogInformation("[Processor] Processed {ImportType} import with validation delay of {DelayMs}ms", 
                importType, delayMs);
        }
    }
}
