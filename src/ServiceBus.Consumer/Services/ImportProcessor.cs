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

            // 2. Calculate simulated row count based on file size
            int totalRows = command.FileSizeBytes > 0 ? (int)(command.FileSizeBytes / 50) : 100;
            _logger.LogInformation("[Processor] Processing {RowCount} rows in parallel", totalRows);

            // 3. Process rows in parallel (simulate concurrent row processing)
            int batchSize = Math.Min(10, totalRows); // Process up to 10 rows at a time
            int batches = (int)Math.Ceiling((double)totalRows / batchSize);
            
            _logger.LogInformation("[Processor] Processing in {BatchCount} batches of {BatchSize} rows each", batches, batchSize);

            for (int batch = 0; batch < batches; batch++)
            {
                int rowsInThisBatch = Math.Min(batchSize, totalRows - (batch * batchSize));
                int rowsProcessedSoFar = batch * batchSize;
                double progressPercentage = ((double)rowsProcessedSoFar / totalRows) * 100;
                
                _logger.LogInformation("[Processor] Progress: {ProgressPercent}% ({RowsProcessed}/{TotalRows} rows) - Starting batch {BatchNumber}/{TotalBatches}", 
                    progressPercentage.ToString("F1"), rowsProcessedSoFar, totalRows, batch + 1, batches);
                
                // Simulate parallel processing of rows in this batch
                var tasks = new Task[rowsInThisBatch];
                for (int i = 0; i < rowsInThisBatch; i++)
                {
                    tasks[i] = ProcessRowAsync(command.ImportType, cancellationToken);
                }
                
                await Task.WhenAll(tasks);
                
                rowsProcessedSoFar += rowsInThisBatch;
                progressPercentage = ((double)rowsProcessedSoFar / totalRows) * 100;
                
                _logger.LogInformation("[Processor] Progress: {ProgressPercent}% ({RowsProcessed}/{TotalRows} rows) - Completed batch {BatchNumber}/{TotalBatches}", 
                    progressPercentage.ToString("F1"), rowsProcessedSoFar, totalRows, batch + 1, batches);
            }

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

        private async Task ProcessRowAsync(string importType, CancellationToken cancellationToken)
        {
            // Simulate processing time per row based on import type
            int delayMs = importType switch
            {
                "Country" => 10,
                "Port" => 15,
                "Vessel" => 20,
                _ => 12
            };

            await Task.Delay(delayMs, cancellationToken);
        }
    }
}
