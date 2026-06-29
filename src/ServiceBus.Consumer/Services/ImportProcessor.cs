using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ServiceBus.Contracts.Commands;
using ServiceBus.Contracts.Events;

namespace ServiceBus.Consumer.Services
{
    public class ImportProcessor : IImportProcessor
    {
        private readonly ILogger<ImportProcessor> _logger;

        public ImportProcessor(ILogger<ImportProcessor> logger)
        {
            _logger = logger;
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

            // 2. Simulate file processing time based on import type
            int delayMs = command.ImportType switch
            {
                "Country" => 1000,
                "Port" => 2000,
                "Vessel" => 3000,
                _ => 1500
            };

            await Task.Delay(delayMs, cancellationToken);

            // 3. Calculate simulated row count based on file size
            int totalRows = command.FileSizeBytes > 0 ? (int)(command.FileSizeBytes / 50) : 100;

            stopwatch.Stop();
            _logger.LogInformation("[Processor] Successfully finished processing job: {CommandId} in {ElapsedMs}ms. Processed {RowCount} rows.",
                command.CommandId, stopwatch.ElapsedMilliseconds, totalRows);

            return new ImportCompletedEvent
            {
                CommandId = command.CommandId,
                ImportType = command.ImportType,
                Status = "Completed",
                TotalRowsProcessed = totalRows,
                ErrorCount = 0,
                ErrorMessage = null,
                CompletedAt = DateTime.UtcNow,
                Duration = stopwatch.Elapsed
            };
        }
    }
}
