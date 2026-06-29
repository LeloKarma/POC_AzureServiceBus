using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Http;
using Newtonsoft.Json;
using ServiceBus.Contracts.Events;
using SysJson = System.Text.Json;

namespace ServiceBus.Consumer.Workers
{
    public class FailureAlertWorker : BackgroundService
    {
        private readonly ServiceBusClient _client;
        private readonly ILogger<FailureAlertWorker> _logger;
        private readonly string _topicName;
        private readonly string _subscriptionName;
        private readonly string? _teamsWebhookUrl;
        private readonly HttpClient _httpClient;
        private ServiceBusProcessor? _processor;

        public FailureAlertWorker(
            ServiceBusClient client,
            IConfiguration configuration,
            ILogger<FailureAlertWorker> logger,
            IHttpClientFactory httpClientFactory)
        {
            _client = client;
            _logger = logger;
            _topicName = configuration["ServiceBus:TopicName"] ?? "masterdata-import-events";
            _subscriptionName = configuration["ServiceBus:Subscriptions:Notification"] ?? "notification-sub";
            _teamsWebhookUrl = configuration["Teams:WebhookUrl"];
            _httpClient = httpClientFactory.CreateClient();
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

        private async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            
            try
            {
                var completedEvent = SysJson.JsonSerializer.Deserialize<ImportCompletedEvent>(body);
                if (completedEvent != null)
                {
                    _logger.LogCritical("🚨 [FailureSub] ALERT ALERTE - IMPORT FAILED! Job: {CommandId}, Type: {ImportType}, Error: {Error}",
                        completedEvent.CommandId, completedEvent.ImportType, completedEvent.ErrorMessage ?? "Unknown failure reason");

                    // Send Teams webhook notification if configured
                    if (!string.IsNullOrEmpty(_teamsWebhookUrl))
                    {
                        await SendTeamsNotificationAsync(completedEvent);
                    }
                    else
                    {
                        _logger.LogWarning("[FailureSub] Teams webhook URL not configured. Skipping notification.");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FailureSub] Failed to process event body.");
            }
        }

        private async Task SendTeamsNotificationAsync(ImportCompletedEvent completedEvent)
        {
            try
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

                var response = await _httpClient.PostAsync(_teamsWebhookUrl, content);
                
                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation("[FailureSub] Teams notification sent successfully for job {CommandId}", completedEvent.CommandId);
                }
                else
                {
                    _logger.LogWarning("[FailureSub] Failed to send Teams notification. Status: {StatusCode}", response.StatusCode);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[FailureSub] Exception while sending Teams notification for job {CommandId}", completedEvent.CommandId);
            }
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
