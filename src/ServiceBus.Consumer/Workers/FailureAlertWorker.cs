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
    public class FailureAlertWorker : BackgroundService
    {
        private readonly ServiceBusClient _client;
        private readonly ILogger<FailureAlertWorker> _logger;
        private readonly string _topicName;
        private readonly string _subscriptionName;
        private ServiceBusProcessor? _processor;

        public FailureAlertWorker(
            ServiceBusClient client,
            IConfiguration configuration,
            ILogger<FailureAlertWorker> logger)
        {
            _client = client;
            _logger = logger;
            _topicName = configuration["ServiceBus:TopicName"] ?? "masterdata-import-events";
            _subscriptionName = configuration["ServiceBus:Subscriptions:Notification"] ?? "notification-sub";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("[FailureSub] Initializing subscription processor for Topic/Sub: {Topic}/{Sub}...", 
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
                    _logger.LogCritical("🚨 [FailureSub] ALERT ALERTE - IMPORT FAILED! Job: {CommandId}, Type: {ImportType}, Error: {Error}",
                        completedEvent.CommandId, completedEvent.ImportType, completedEvent.ErrorMessage ?? "Unknown failure reason");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FailureSub] Failed to process event body.");
            }

            return Task.CompletedTask;
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "[FailureSub] Error in subscription source: {ErrorSource}", args.ErrorSource);
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
