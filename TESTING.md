# Testing Guide - Azure Service Bus POC

This guide provides step-by-step instructions for testing the Azure Service Bus Master Data Import POC.

## Prerequisites

- Azure Service Bus namespace with the following resources:
  - Queue: `masterdata-import-queue`
  - Topic: `masterdata-import-events`
  - Subscriptions: `history-sub`, `audit-sub`, `notification-sub`
- .NET 10.0 SDK
- Azure credentials (connection string or DefaultAzureCredential)
- (Optional) Microsoft Teams webhook URL for failure notifications

## Configuration

### 1. Configure Consumer (appsettings.json)

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
  },
  "Teams": {
    "WebhookUrl": "YOUR_TEAMS_WEBHOOK_URL"
  }
}
```

### 2. Configure Producer (appsettings.json)

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

**Note:** You can also use environment variable `AZURE_SERVICEBUS_CONNECTION_STRING` instead of appsettings.

## Running the Applications

### Start the Consumer

```bash
cd c:\POC_ServiceBus\src\ServiceBus.Consumer
dotnet run
```

The Consumer will:
- Start 4 background workers (Queue + 3 Subscriptions)
- Log initialization messages for each worker
- Process messages from the queue and subscriptions

### Start the Producer

```bash
cd c:\POC_ServiceBus\src\ServiceBus.Producer
dotnet run
```

The Producer will:
- Start the Web API on `http://localhost:5000`
- Expose Swagger UI at `http://localhost:5000/swagger`

## Testing Features

### 1. Test Basic Import (Single Import)

**Endpoint:** `POST /api/import/upload`

**Parameters:**
- `importType`: Import type (Country, Port, Vessel, FAIL_TRIGGER)
- `userId`: User ID (integer)
- `fileName`: File name (optional)
- `fileSizeBytes`: File size in bytes (optional)

**Examples:**

```bash
# Successful Country import
curl -X POST "http://localhost:5000/api/import/upload?importType=Country&userId=1&fileName=countries.xlsx&fileSizeBytes=1024000"

# Successful Port import
curl -X POST "http://localhost:5000/api/import/upload?importType=Port&userId=1&fileName=ports.xlsx&fileSizeBytes=512000"

# Trigger failure (for testing retry and Teams notification)
curl -X POST "http://localhost:5000/api/import/upload?importType=FAIL_TRIGGER&userId=1&fileName=test.xlsx&fileSizeBytes=1000"
```

**Expected Results:**
- Consumer logs: Import started, parallel processing simulation, progress logs
- HistorySub logs: Import completion history
- AuditSub logs: Audit trail
- FailureSub logs: Critical failure alert (for FAIL_TRIGGER)
- Teams notification: Sent if webhook URL configured (for failures)

### 2. Test Validation

**Test invalid import type:**
```bash
curl -X POST "http://localhost:5000/api/import/upload?importType=InvalidType&userId=1"
```
**Expected:** 400 Bad Request with validation error

**Test file size too large (50MB limit):**
```bash
curl -X POST "http://localhost:5000/api/import/upload?importType=Country&userId=1&fileSizeBytes=52428801"
```
**Expected:** 400 Bad Request with file size error

**Test empty import type:**
```bash
curl -X POST "http://localhost:5000/api/import/upload?importType=&userId=1"
```
**Expected:** 400 Bad Request

### 3. Test Bulk Operations

**Endpoint:** `POST /api/import/bulk`

**Body:**
```json
{
  "requests": [
    { "importType": "Country", "fileName": "countries1.xlsx", "fileSizeBytes": 1000000, "userId": 1 },
    { "importType": "Port", "fileName": "ports1.xlsx", "fileSizeBytes": 500000, "userId": 1 },
    { "importType": "Vessel", "fileName": "vessels1.xlsx", "fileSizeBytes": 2000000, "userId": 1 }
  ]
}
```

**Test with invalid request:**
```json
{
  "requests": [
    { "importType": "Country", "fileName": "countries1.xlsx", "fileSizeBytes": 1000000, "userId": 1 },
    { "importType": "InvalidType", "fileName": "bad.xlsx", "fileSizeBytes": 1000, "userId": 1 }
  ]
}
```

**Expected:** Mixed results - some successful, some with validation errors

**Test exceeding 100 request limit:**
```json
{
  "requests": [ ... 101 requests ... ]
}
```
**Expected:** 400 Bad Request - exceeds limit

### 4. Test Exponential Backoff Retry

1. Send a `FAIL_TRIGGER` import:
```bash
curl -X POST "http://localhost:5000/api/import/upload?importType=FAIL_TRIGGER&userId=1"
```

2. Observe Consumer logs:
   - First attempt fails (delivery count 1)
   - Exponential backoff: 2^1 = 2 seconds delay
   - Second attempt fails (delivery count 2)
   - Exponential backoff: 2^2 = 4 seconds delay
   - Third attempt fails (delivery count 3)
   - Exponential backoff: 2^3 = 8 seconds delay
   - ... up to 30 seconds max delay
   - After max attempts, message sent to Dead-Letter Queue

3. Check Dead-Letter Queue:
```bash
curl -X GET "http://localhost:5000/api/import/dlq"
```

### 5. Test Dead-Letter Queue Inspection

**Endpoint:** `GET /api/import/dlq`

```bash
curl -X GET "http://localhost:5000/api/import/dlq"
```

**Expected:** Returns messages currently in the DLQ (after failed retries)

### 6. Test Teams Webhook Notification

1. Configure Teams webhook URL in Consumer appsettings.json
2. Send a `FAIL_TRIGGER` import
3. Check Microsoft Teams channel for adaptive card notification

**Expected:** Teams notification with:
- Job ID
- Import Type
- Status (Failed)
- Error message
- Retry count
- Failed timestamp

## Monitoring Logs

### Consumer Logs

Look for these log patterns:

**Import Processing:**
```
[ImportQueueWorker] Received import command: CommandId=..., ImportType=Country
[ImportProcessor] Processing import: CommandId=..., TotalRows=1000
[ImportProcessor] Progress: 10% (100/1000 rows)
[ImportProcessor] Progress: 20% (200/1000 rows)
...
[ImportProcessor] Processing complete: CommandId=..., Duration=...
```

**History Subscription:**
```
💚 [HistorySub] LOGGED HISTORY: Import ... successfully imported ... rows in ...ms. Retries: ..., CompletedAt: ...
```

**Audit Subscription:**
```
🔍 [AuditSub] AUDIT TRAIL LOG: Job ... finalized with Status: ... Rows: ..., Duration: ...ms, Retries: ...
```

**Failure Subscription:**
```
🚨 [FailureSub] ALERT ALERTE - IMPORT FAILED! Job: ..., Type: ..., Error: ...
[FailureSub] Teams notification sent successfully for job ...
```

**Retry Logic:**
```
[ImportQueueWorker] Message failed, delivery count: 1, retrying in 2s...
[ImportQueueWorker] Message failed, delivery count: 2, retrying in 4s...
[ImportQueueWorker] Message failed, delivery count: 3, retrying in 8s...
```

### Producer Logs

Look for:
```
[Producer] Import command enqueued: CommandId=..., ImportType=Country
[Validation] Import type validated: Country
[Bulk] Processing 3 bulk requests
```

## Cleanup

After testing, you may want to:

1. Clear the queue:
```bash
# Use Azure Portal or Service Bus Explorer to purge messages
```

2. Clear the Dead-Letter Queue:
```bash
# Use Azure Portal or Service Bus Explorer to purge DLQ
```

3. Stop both applications (Ctrl+C in each terminal)

## Troubleshooting

### Connection Issues
- Verify connection string is correct
- Check if Service Bus namespace exists
- Ensure firewall allows outbound connections

### No Messages Processed
- Check Consumer is running
- Verify queue/topic names match configuration
- Check Service Bus Explorer for message presence

### Teams Notification Not Sent
- Verify webhook URL is configured
- Check webhook URL is valid and accessible
- Check Consumer logs for webhook errors

### Retries Not Working
- Verify message is failing (use FAIL_TRIGGER)
- Check delivery count in logs
- Ensure exponential backoff logic is executing
