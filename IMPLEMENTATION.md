# Implementation Guide - Azure Service Bus POC

This document provides detailed implementation information for the Azure Service Bus Master Data Import POC.

## Architecture Overview

The POC implements a producer-consumer pattern using Azure Service Bus for asynchronous master data imports.

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Producer API                              │
│  (ASP.NET Core Web API - ImportController)                  │
│  - Validation                                                │
│  - Bulk Operations                                          │
│  - Message Publishing                                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │ Azure Service Bus     │
         │ - Queue:               │
         │   masterdata-import-queue │
         │ - Topic:               │
         │   masterdata-import-events │
         │ - Subscriptions:       │
         │   history-sub          │
         │   audit-sub            │
         │   notification-sub     │
         └───────────┬───────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                     Consumer Worker Service                   │
│  (Background Service - 4 Workers)                           │
│  - ImportQueueWorker (Queue)                                │
│  - HistorySubscriptionWorker (history-sub)                   │
│  - AuditSubscriptionWorker (audit-sub)                       │
│  - FailureAlertWorker (notification-sub)                    │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
POC_ServiceBus/
├── src/
│   ├── ServiceBus.Contracts/          # Shared contracts
│   │   ├── Commands/
│   │   │   └── ImportCommand.cs      # Import command message
│   │   └── Events/
│   │       └── ImportCompletedEvent.cs # Import completion event
│   ├── ServiceBus.Producer/          # Producer API
│   │   ├── Controllers/
│   │   │   └── ImportController.cs   # REST API endpoints
│   │   ├── Services/
│   │   │   └── ServiceBusPublisher.cs # Message publisher
│   │   └── Program.cs                # API startup
│   └── ServiceBus.Consumer/          # Consumer Worker
│       ├── Workers/
│       │   ├── ImportQueueWorker.cs  # Queue message processor
│       │   ├── HistorySubscriptionWorker.cs # History logger
│       │   ├── AuditSubscriptionWorker.cs   # Audit logger
│       │   └── FailureAlertWorker.cs # Failure notification
│       ├── Services/
│       │   ├── IImportProcessor.cs  # Processor interface
│       │   └── ImportProcessor.cs   # Import processing logic
│       └── Program.cs                # Worker startup
```

## Key Features Implementation

### 1. Input Validation

**Location:** `ServiceBus.Producer/Controllers/ImportController.cs`

**Implementation:**
- Validates import type against allowed values: `Country`, `Port`, `Vessel`, `FAIL_TRIGGER`
- Validates file size against maximum limit (50MB)
- Custom validation rules per import type
- Returns 400 Bad Request with descriptive error messages

**Code:**
```csharp
private static readonly string[] ValidImportTypes = { "Country", "Port", "Vessel", "FAIL_TRIGGER" };
private const long MaxFileSizeBytes = 50 * 1024 * 1024; // 50MB

private IActionResult ValidateImportRequest(string importType, long fileSize)
{
    if (string.IsNullOrWhiteSpace(importType))
        return BadRequest("Import type is required.");
    
    if (!ValidImportTypes.Contains(importType))
        return BadRequest($"Invalid import type. Allowed: {string.Join(", ", ValidImportTypes)}");
    
    if (fileSize > MaxFileSizeBytes)
        return BadRequest($"File size exceeds maximum of {MaxFileSizeBytes / (1024 * 1024)}MB.");
    
    return null;
}
```

### 2. Exponential Backoff Retry

**Location:** `ServiceBus.Consumer/Workers/ImportQueueWorker.cs`

**Implementation:**
- Calculates delay as `2^deliveryCount` seconds
- Caps maximum delay at 30 seconds
- Abandons message after delay to trigger retry
- Tracks retry count in `ImportCompletedEvent`

**Code:**
```csharp
private async Task ProcessWithRetryAsync(ServiceBusReceivedMessage message, ImportCommand command)
{
    int deliveryCount = message.DeliveryCount;
    int delaySeconds = (int)Math.Min(Math.Pow(2, deliveryCount), 30);
    
    _logger.LogWarning("[ImportQueueWorker] Message failed, delivery count: {DeliveryCount}, retrying in {Delay}s...", 
        deliveryCount, delaySeconds);
    
    await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
    await _receiver.AbandonMessageAsync(message);
}
```

### 3. Retry Count Tracking

**Location:** `ServiceBus.Contracts/Events/ImportCompletedEvent.cs`

**Implementation:**
- Added `RetryCount` property to track retry attempts
- Populated on both success and failure scenarios

**Code:**
```csharp
public class ImportCompletedEvent
{
    public string CommandId { get; set; } = Guid.NewGuid().ToString();
    public string ImportType { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public int TotalRowsProcessed { get; set; }
    public int ErrorCount { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime CompletedAt { get; set; }
    public TimeSpan Duration { get; set; }
    public int RetryCount { get; set; } = 0; // Track retry attempts
}
```

### 4. Bulk Operations

**Location:** `ServiceBus.Producer/Controllers/ImportController.cs`

**Implementation:**
- Accepts up to 100 import requests in single call
- Validates each request individually
- Returns detailed results for each request
- Parallel message publishing for efficiency

**Code:**
```csharp
[HttpPost("bulk")]
public async Task<IActionResult> BulkEnqueue([FromBody] BulkImportRequest bulkRequest)
{
    if (bulkRequest.Requests.Count > 100)
        return BadRequest("Maximum 100 requests per bulk operation.");
    
    var results = new List<object>();
    
    foreach (var request in bulkRequest.Requests)
    {
        var validation = ValidateImportRequest(request.ImportType, request.FileSizeBytes);
        if (validation != null)
        {
            results.Add(new { request.ImportType, Error = "Validation failed" });
            continue;
        }
        
        var command = new ImportCommand
        {
            CommandId = Guid.NewGuid().ToString(),
            ImportType = request.ImportType,
            FileName = request.FileName,
            FileSizeBytes = request.FileSizeBytes,
            UserId = request.UserId
        };
        
        await _publisher.PublishImportCommandAsync(command);
        results.Add(new { request.ImportType, CommandId = command.CommandId });
    }
    
    return Accepted(new { Message = $"Processed {results.Count} requests.", Results = results });
}
```

### 5. Parallel Row Processing Simulation

**Location:** `ServiceBus.Consumer/Services/ImportProcessor.cs`

**Implementation:**
- Simulates processing of configurable row count (default 1000)
- Processes in batches of 100 rows
- Reports progress at 10% intervals
- Uses `Task.WhenAll` for parallel batch processing

**Code:**
```csharp
public async Task<ImportCompletedEvent> ProcessImportAsync(ImportCommand command)
{
    var totalRows = command.ImportType == "Country" ? 1000 : 500;
    var batchSize = 100;
    var batches = (int)Math.Ceiling((double)totalRows / batchSize);
    
    for (int i = 0; i < batches; i++)
    {
        var batchTasks = Enumerable.Range(0, batchSize)
            .Select(async _ =>
            {
                await Task.Delay(10); // Simulate row processing
            });
        
        await Task.WhenAll(batchTasks);
        
        int processedRows = (i + 1) * batchSize;
        int progress = Math.Min((int)((double)processedRows / totalRows * 100), 100);
        
        _logger.LogInformation("[ImportProcessor] Progress: {Progress}% ({Processed}/{Total} rows)", 
            progress, processedRows, totalRows);
    }
    
    return new ImportCompletedEvent { /* ... */ };
}
```

### 6. Progress Reporting

**Location:** `ServiceBus.Consumer/Services/ImportProcessor.cs`

**Implementation:**
- Logs progress at 10%, 20%, 30%, ..., 100%
- Shows processed/total row counts
- Includes CommandId for tracking

**Code:**
```csharp
_logger.LogInformation("[ImportProcessor] Progress: {Progress}% ({Processed}/{Total} rows) - CommandId: {CommandId}", 
    progress, processedRows, totalRows, command.CommandId);
```

### 7. Microsoft Teams Webhook Notification

**Location:** `ServiceBus.Consumer/Workers/FailureAlertWorker.cs`

**Implementation:**
- Subscribes to `notification-sub` (filter: Status = Failed)
- Sends adaptive card notification on import failure
- Configured via `Teams:WebhookUrl` in appsettings
- Includes job details in notification

**Code:**
```csharp
private async Task SendTeamsNotificationAsync(ImportCompletedEvent completedEvent)
{
    var teamsMessage = new
    {
        type = "message",
        attachments = new object[]
        {
            new
            {
                contentType = "application/vnd.microsoft.card.adaptive",
                contentUrl = (object?)null,
                content = new
                {
                    type = "AdaptiveCard",
                    schema = "http://adaptivecards.io/schemas/adaptive-card.json",
                    version = "1.4",
                    body = new object[]
                    {
                        new
                        {
                            type = "TextBlock",
                            text = "🚨 Import Failed Alert",
                            weight = "Bolder",
                            size = "Large",
                            color = "Attention"
                        },
                        new
                        {
                            type = "FactSet",
                            facts = new object[]
                            {
                                new { title = "Job ID", value = completedEvent.CommandId },
                                new { title = "Import Type", value = completedEvent.ImportType },
                                new { title = "Status", value = completedEvent.Status },
                                new { title = "Error", value = completedEvent.ErrorMessage ?? "Unknown" },
                                new { title = "Retry Count", value = completedEvent.RetryCount.ToString() },
                                new { title = "Failed At", value = completedEvent.CompletedAt.ToString("yyyy-MM-dd HH:mm:ss") }
                            }
                        }
                    }
                }
            }
        }
    };

    var jsonPayload = JsonConvert.SerializeObject(teamsMessage);
    var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
    await _httpClient.PostAsync(_teamsWebhookUrl, content);
}
```

### 8. Enhanced Logging

**Locations:** All worker files

**Implementation:**
- Structured logging with context (CommandId, ImportType, Status)
- Retry count and completion timestamps in subscription workers
- Critical log level for failures
- Warning log level for configuration issues

**Examples:**
```csharp
_logger.LogInformation("💚 [HistorySub] LOGGED HISTORY: Import {CommandId} (Type: {ImportType}) successfully imported {RowCount} rows in {DurationMs}ms. Retries: {RetryCount}, CompletedAt: {CompletedAt}", ...);
_logger.LogCritical("🚨 [FailureSub] ALERT ALERTE - IMPORT FAILED! Job: {CommandId}, Type: {ImportType}, Error: {Error}", ...);
```

## Message Flow

### Successful Import Flow

```
1. Producer API receives import request
   ↓
2. Validation (import type, file size)
   ↓
3. Create ImportCommand with CommandId
   ↓
4. Publish to Service Bus Queue
   ↓
5. Consumer ImportQueueWorker receives message
   ↓
6. ImportProcessor processes import (parallel simulation)
   ↓
7. Publish ImportCompletedEvent to Topic (Status = Completed)
   ↓
8. HistorySubscriptionWorker logs completion history
   ↓
9. AuditSubscriptionWorker logs audit trail
```

### Failed Import Flow

```
1. Producer API receives import request
   ↓
2. Validation
   ↓
3. Publish to Service Bus Queue
   ↓
4. Consumer ImportQueueWorker receives message
   ↓
5. ImportProcessor fails (FAIL_TRIGGER or actual error)
   ↓
6. Exponential backoff retry (2^deliveryCount seconds)
   ↓
7. After max retries, publish ImportCompletedEvent (Status = Failed)
   ↓
8. HistorySubscriptionWorker logs failure
   ↓
9. AuditSubscriptionWorker logs audit trail
   ↓
10. FailureAlertWorker sends Teams notification
   ↓
11. Message sent to Dead-Letter Queue
```

## Configuration

### Service Bus Configuration

**Consumer appsettings.json:**
```json
{
  "ServiceBus": {
    "ConnectionString": "YOUR_CONNECTION_STRING",
    "QueueName": "masterdata-import-queue",
    "TopicName": "masterdata-import-events",
    "Subscriptions": {
      "History": "history-sub",
      "Audit": "audit-sub",
      "Notification": "notification-sub"
    },
    "FullyQualifiedNamespace": "POCservicebusTests.servicebus.windows.net"
  }
}
```

**Producer appsettings.json:**
```json
{
  "ServiceBus": {
    "ConnectionString": "YOUR_CONNECTION_STRING",
    "QueueName": "masterdata-import-queue",
    "TopicName": "masterdata-import-events",
    "FullyQualifiedNamespace": "POCservicebusTests.servicebus.windows.net"
  }
}
```

### Teams Webhook Configuration

**Consumer appsettings.json:**
```json
{
  "Teams": {
    "WebhookUrl": "YOUR_TEAMS_WEBHOOK_URL"
  }
}
```

## Dependencies

### Producer Project
- `Azure.Identity` - Azure authentication
- `Azure.Messaging.ServiceBus` - Service Bus client
- `Swashbuckle.AspNetCore` - Swagger/OpenAPI

### Consumer Project
- `Azure.Identity` - Azure authentication
- `Azure.Messaging.ServiceBus` - Service Bus client
- `Microsoft.Extensions.Hosting` - Background service hosting
- `Microsoft.Extensions.Http` - HTTP client factory
- `Newtonsoft.Json` - JSON serialization for Teams payload

## Deployment Considerations

### Environment Variables
- Use `AZURE_SERVICEBUS_CONNECTION_STRING` for connection string
- Use `Teams:WebhookUrl` for Teams webhook (optional)

### Scaling
- Consumer: Scale horizontally by running multiple instances
- Producer: Scale horizontally behind load balancer
- Service Bus: Auto-scales based on throughput

### Monitoring
- Enable Application Insights for distributed tracing
- Monitor queue length and topic subscription lag
- Track retry counts and failure rates
- Monitor Teams webhook delivery success rate

## Error Handling

### Producer Errors
- Validation errors: Return 400 with details
- Service Bus errors: Return 500 with error message
- Bulk operation errors: Partial success with detailed results

### Consumer Errors
- Message processing errors: Retry with exponential backoff
- Max retries exceeded: Send to Dead-Letter Queue
- Subscription errors: Log and continue processing
- Teams webhook errors: Log but don't fail message processing

## Security Considerations

- Use Managed Identity or connection string from Key Vault
- Validate all input parameters
- Implement rate limiting on Producer API
- Use HTTPS for all API calls
- Secure Teams webhook URL (do not commit to source control)
