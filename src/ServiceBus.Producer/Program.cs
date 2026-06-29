using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ServiceBus.Producer.Services;
using System;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();

// Add Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new Microsoft.OpenApi.Models.OpenApiInfo
    {
        Title = "Service Bus Producer API",
        Version = "v1",
        Description = "API for triggering master data imports via Azure Service Bus"
    });
});

// Configure Service Bus Client
builder.Services.AddSingleton(sp =>
{
    var config = sp.GetRequiredService<Microsoft.Extensions.Configuration.IConfiguration>();
    string? connectionString = Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING") 
                              ?? config["ServiceBus:ConnectionString"];
    
    if (!string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("[Producer] Registering ServiceBusClient using connection string.");
        return new ServiceBusClient(connectionString);
    }
    else
    {
        string namespaceName = config["ServiceBus:FullyQualifiedNamespace"] ?? "POCservicebusTests.servicebus.windows.net";
        Console.WriteLine($"[Producer] Registering ServiceBusClient using DefaultAzureCredential with namespace: {namespaceName}");
        return new ServiceBusClient(namespaceName, new DefaultAzureCredential());
    }
});

// Register publisher
builder.Services.AddSingleton<IServiceBusPublisher, ServiceBusPublisher>();

// Register blob storage service
builder.Services.AddSingleton<IBlobStorageService, BlobStorageService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Service Bus Producer API v1");
    });
}

app.UseRouting();
app.UseAuthorization();
app.MapControllers();

app.Run();
