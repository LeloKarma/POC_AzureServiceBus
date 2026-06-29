using System;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ServiceBus.Contracts.Commands;
using ServiceBus.Contracts.Events;

namespace ServiceBus.Producer.Services
{
    public class ServiceBusPublisher : IServiceBusPublisher, IAsyncDisposable
    {
        private readonly ServiceBusClient _client;
        private readonly ServiceBusSender _queueSender;
        private readonly ServiceBusSender _topicSender;
        private readonly ILogger<ServiceBusPublisher> _logger;

        public ServiceBusPublisher(
            ServiceBusClient client,
            IConfiguration configuration,
            ILogger<ServiceBusPublisher> logger)
        {
            _client = client;
            _logger = logger;

            string queueName = configuration["ServiceBus:QueueName"] ?? "masterdata-import-queue";
            string topicName = configuration["ServiceBus:TopicName"] ?? "masterdata-import-events";

            _queueSender = _client.CreateSender(queueName);
            _topicSender = _client.CreateSender(topicName);
        }

        public async Task SendCommandAsync(ImportMasterDataCommand command)
        {
            try
            {
                string jsonPayload = JsonSerializer.Serialize(command);
                var message = new ServiceBusMessage(jsonPayload)
                {
                    MessageId = command.CommandId,
                    ContentType = "application/json",
                    Subject = "ImportMasterDataCommand"
                };

                // Add custom application properties
                message.ApplicationProperties.Add("ImportType", command.ImportType);
                message.ApplicationProperties.Add("RequestedBy", command.RequestedBy);

                _logger.LogInformation("Sending ImportMasterDataCommand {CommandId} (Type: {ImportType}) to Queue...", 
                    command.CommandId, command.ImportType);

                await _queueSender.SendMessageAsync(message);

                _logger.LogInformation("ImportMasterDataCommand {CommandId} sent successfully.", command.CommandId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send command to Service Bus Queue.");
                throw;
            }
        }

        public async Task PublishEventAsync(ImportCompletedEvent completedEvent)
        {
            try
            {
                string jsonPayload = JsonSerializer.Serialize(completedEvent);
                var message = new ServiceBusMessage(jsonPayload)
                {
                    MessageId = Guid.NewGuid().ToString("N"),
                    CorrelationId = completedEvent.CommandId,
                    ContentType = "application/json",
                    Subject = "ImportCompletedEvent"
                };

                // Add custom application properties (used by SQL Filters in Subscriptions)
                message.ApplicationProperties.Add("Status", completedEvent.Status);
                message.ApplicationProperties.Add("ImportType", completedEvent.ImportType);

                _logger.LogInformation("Publishing ImportCompletedEvent for Command {CommandId} (Status: {Status}) to Topic...", 
                    completedEvent.CommandId, completedEvent.Status);

                await _topicSender.SendMessageAsync(message);

                _logger.LogInformation("ImportCompletedEvent published successfully for Command {CommandId}.", completedEvent.CommandId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish event to Service Bus Topic.");
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _queueSender.DisposeAsync();
            await _topicSender.DisposeAsync();
        }
    }
}
