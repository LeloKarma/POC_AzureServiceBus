using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ServiceBus.Contracts.Events;

namespace ServiceBus.Consumer.Workers
{
    public class AuditSubscriptionWorker : BackgroundService
    {
        private readonly ServiceBusClient _client;
        private readonly ILogger<AuditSubscriptionWorker> _logger;
        private readonly string _topicName;
        private readonly string _subscriptionName;
        private ServiceBusProcessor? _processor;

        public AuditSubscriptionWorker(
            ServiceBusClient client,
            IConfiguration configuration,
            ILogger<AuditSubscriptionWorker> logger)
        {
            _client = client;
            _logger = logger;
            _topicName = configuration["ServiceBus:TopicName"] ?? "masterdata-import-events";
            _subscriptionName = configuration["ServiceBus:Subscriptions:Audit"] ?? "audit-sub";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("[AuditSub] Initializing subscription processor for Topic/Sub: {Topic}/{Sub}...", 
                _topicName, _subscriptionName);

            var options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = true,
                MaxConcurrentCalls = 1
            };

            _processor = _client.CreateProcessor(_topicName, _subscriptionName, options);
            _processor.ProcessMessageAsync += MessageHandler;
            _processor.ProcessErrorAsync += ErrorHandler;

            await _processor.StartProcessingAsync(stoppingToken);
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        private Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            
            try
            {
                var completedEvent = JsonSerializer.Deserialize<ImportCompletedEvent>(body);
                if (completedEvent != null)
                {
                    _logger.LogInformation("🔍 [AuditSub] AUDIT TRAIL LOG: Job {CommandId} (Type: {ImportType}) finalized with Status: {Status}. Rows: {RowCount}, Duration: {DurationMs}ms, Retries: {RetryCount}, CompletedAt: {CompletedAt}",
                        completedEvent.CommandId, completedEvent.ImportType, completedEvent.Status, completedEvent.TotalRowsProcessed, completedEvent.Duration.TotalMilliseconds, completedEvent.RetryCount, completedEvent.CompletedAt);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[AuditSub] Failed to process event body.");
            }

            return Task.CompletedTask;
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "[AuditSub] Error in subscription source: {ErrorSource}", args.ErrorSource);
            return Task.CompletedTask;
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_processor != null)
            {
                await _processor.StopProcessingAsync(cancellationToken);
                await _processor.DisposeAsync();
            }
            await base.StopAsync(cancellationToken);
        }
    }
}
