using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ServiceBus.Contracts.Commands;
using ServiceBus.Contracts.Events;
using ServiceBus.Consumer.Services;

namespace ServiceBus.Consumer.Workers
{
    public class ImportQueueWorker : BackgroundService
    {
        private readonly ServiceBusClient _client;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ImportQueueWorker> _logger;
        private readonly string _queueName;
        private readonly string _topicName;
        private ServiceBusProcessor? _processor;
        private ServiceBusSender? _topicSender;

        public ImportQueueWorker(
            ServiceBusClient client,
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration,
            ILogger<ImportQueueWorker> logger)
        {
            _client = client;
            _scopeFactory = scopeFactory;
            _logger = logger;
            _queueName = configuration["ServiceBus:QueueName"] ?? "masterdata-import-queue";
            _topicName = configuration["ServiceBus:TopicName"] ?? "masterdata-import-events";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("[QueueWorker] Initializing Service Bus Processor for queue '{QueueName}'...", _queueName);

            _topicSender = _client.CreateSender(_topicName);

            // Configure processor with PeekLock and concurrency level of 2 (Competing Consumers)
            var processorOptions = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false, // We handle completion manually (at-least-once / peek-lock)
                MaxConcurrentCalls = 2,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            };

            _processor = _client.CreateProcessor(_queueName, processorOptions);

            _processor.ProcessMessageAsync += MessageHandler;
            _processor.ProcessErrorAsync += ErrorHandler;

            _logger.LogInformation("[QueueWorker] Starting message processing...");
            await _processor.StartProcessingAsync(stoppingToken);

            // Keep the worker running until cancellation is requested
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        private async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            _logger.LogInformation("[QueueWorker] Received message ID: {MessageId}, DeliveryCount: {DeliveryCount}", 
                args.Message.MessageId, args.Message.DeliveryCount);

            ImportMasterDataCommand? command = null;
            try
            {
                command = JsonSerializer.Deserialize<ImportMasterDataCommand>(body);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[QueueWorker] Failed to deserialize message body. Moving to DLQ directly.");
                await args.DeadLetterMessageAsync(args.Message, "DeserializationFailed", ex.Message);
                return;
            }

            if (command == null)
            {
                await args.DeadLetterMessageAsync(args.Message, "NullPayload", "The deserialized command is null.");
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<IImportProcessor>();

            try
            {
                // Process the import
                ImportCompletedEvent completedEvent = await processor.ProcessImportAsync(command, args.CancellationToken);

                // Add retry count to the event
                completedEvent = completedEvent with { RetryCount = args.Message.DeliveryCount };

                // Publish completed event to the topic
                await PublishCompletedEventAsync(completedEvent);

                // Successfully processed - complete the message to remove it from the queue
                _logger.LogInformation("[QueueWorker] Completing message ID: {MessageId} (Retries: {RetryCount})", 
                    args.Message.MessageId, args.Message.DeliveryCount);
                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[QueueWorker] Error processing message {MessageId} (Attempt {DeliveryCount}/3)", 
                    args.Message.MessageId, args.Message.DeliveryCount);

                // Implement exponential backoff before retry
                int delayMs = CalculateExponentialBackoff(args.Message.DeliveryCount);
                if (delayMs > 0)
                {
                    _logger.LogInformation("[QueueWorker] Applying exponential backoff: {DelayMs}ms before retry attempt {DeliveryCount}", 
                        delayMs, args.Message.DeliveryCount + 1);
                    await Task.Delay(delayMs, args.CancellationToken);
                }

                // If we have retried 3 times, Service Bus will automatically DLQ it if we abandon it again.
                // Let's log it clearly so the user sees this flow.
                if (args.Message.DeliveryCount >= 3)
                {
                    _logger.LogCritical("[QueueWorker] Message {MessageId} has reached max delivery attempts (3). Service Bus will move it to DLQ.", 
                        args.Message.MessageId);
                    
                    // We can also publish a failed event so other services know it failed
                    var failedEvent = new ImportCompletedEvent
                    {
                        CommandId = command.CommandId,
                        ImportType = command.ImportType,
                        Status = "Failed",
                        TotalRowsProcessed = 0,
                        ErrorCount = 1,
                        ErrorMessage = ex.Message,
                        CompletedAt = DateTime.UtcNow,
                        Duration = TimeSpan.Zero,
                        RetryCount = args.Message.DeliveryCount
                    };
                    await PublishCompletedEventAsync(failedEvent);
                }

                // Abandon the message to make it visible to consumers again
                await args.AbandonMessageAsync(args.Message);
            }
        }

        private async Task PublishCompletedEventAsync(ImportCompletedEvent completedEvent)
        {
            if (_topicSender == null) return;

            string jsonPayload = JsonSerializer.Serialize(completedEvent);
            var message = new ServiceBusMessage(jsonPayload)
            {
                MessageId = Guid.NewGuid().ToString("N"),
                CorrelationId = completedEvent.CommandId,
                ContentType = "application/json",
                Subject = "ImportCompletedEvent"
            };

            // Custom application properties for Subscription Routing (SQL Filters)
            message.ApplicationProperties.Add("Status", completedEvent.Status);
            message.ApplicationProperties.Add("ImportType", completedEvent.ImportType);

            _logger.LogInformation("[QueueWorker] Publishing event to Topic (Status: {Status}, Type: {ImportType})", 
                completedEvent.Status, completedEvent.ImportType);
            
            await _topicSender.SendMessageAsync(message);
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "[QueueWorker] Error in message source: {ErrorSource}, Namespace: {FullyQualifiedNamespace}", 
                args.ErrorSource, args.FullyQualifiedNamespace);
            return Task.CompletedTask;
        }

        private int CalculateExponentialBackoff(int deliveryCount)
        {
            // Exponential backoff: 2^deliveryCount seconds, capped at 30 seconds
            int delaySeconds = (int)Math.Pow(2, deliveryCount);
            int maxDelaySeconds = 30;
            return Math.Min(delaySeconds, maxDelaySeconds) * 1000;
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("[QueueWorker] Stopping message processor...");
            if (_processor != null)
            {
                await _processor.StopProcessingAsync(cancellationToken);
                await _processor.DisposeAsync();
            }

            if (_topicSender != null)
            {
                await _topicSender.DisposeAsync();
            }

            await base.StopAsync(cancellationToken);
        }
    }
}
