using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ServiceBus.Consumer.Services;
using ServiceBus.Consumer.Workers;
using System;

var builder = Host.CreateApplicationBuilder(args);

// Register ServiceBusClient
builder.Services.AddSingleton(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    string? connectionString = Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING") 
                              ?? config["ServiceBus:ConnectionString"];
    
    if (!string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("[Consumer] Registering ServiceBusClient using connection string.");
        return new ServiceBusClient(connectionString);
    }
    else
    {
        string namespaceName = config["ServiceBus:FullyQualifiedNamespace"] ?? "POCservicebusTests.servicebus.windows.net";
        Console.WriteLine($"[Consumer] Registering ServiceBusClient using DefaultAzureCredential with namespace: {namespaceName}");
        return new ServiceBusClient(namespaceName, new DefaultAzureCredential());
    }
});

// Register import processor
builder.Services.AddTransient<IImportProcessor, ImportProcessor>();

// Register blob storage service
builder.Services.AddSingleton<IBlobStorageService, BlobStorageService>();

// Register workers (Queue + 3 Subscriptions)
builder.Services.AddHostedService<ImportQueueWorker>();
builder.Services.AddHostedService<HistorySubscriptionWorker>();
builder.Services.AddHostedService<AuditSubscriptionWorker>();
builder.Services.AddHostedService<FailureAlertWorker>();

var host = builder.Build();
host.Run();
