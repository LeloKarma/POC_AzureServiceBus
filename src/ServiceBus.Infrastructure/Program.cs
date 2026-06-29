using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus.Administration;

namespace ServiceBus.Infrastructure
{
    class Program
    {
        private const string QueueName = "masterdata-import-queue";
        private const string TopicName = "masterdata-import-events";
        private const string HistorySub = "history-sub";
        private const string AuditSub = "audit-sub";
        private const string NotificationSub = "notification-sub";

        static async Task Main(string[] args)
        {
            Console.WriteLine("==================================================");
            Console.WriteLine("Azure Service Bus Infrastructure Setup");
            Console.WriteLine("==================================================");

            // Fetch configuration from Environment Variables or use defaults
            string? connectionString = Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING");
            string? fullyQualifiedNamespace = Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_NAMESPACE") ?? "POCservicebusTests.servicebus.windows.net";

            ServiceBusAdministrationClient adminClient;

            if (!string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("Using connection string authentication.");
                adminClient = new ServiceBusAdministrationClient(connectionString);
            }
            else
            {
                Console.WriteLine($"Using DefaultAzureCredential with namespace: {fullyQualifiedNamespace}");
                // DefaultAzureCredential will try Visual Studio, AZ CLI, Managed Identity, etc.
                adminClient = new ServiceBusAdministrationClient(fullyQualifiedNamespace, new DefaultAzureCredential());
            }

            try
            {
                // 1. Create Queue
                await CreateQueueAsync(adminClient);

                // 2. Create Topic
                await CreateTopicAsync(adminClient);

                // 3. Create Subscriptions and Rules
                await CreateSubscriptionWithFilterAsync(adminClient, HistorySub, "Status = 'Completed'");
                await CreateSubscriptionWithFilterAsync(adminClient, AuditSub, "1=1");
                await CreateSubscriptionWithFilterAsync(adminClient, NotificationSub, "Status = 'Failed'");

                Console.WriteLine("\n[SUCCESS] Infrastructure setup completed successfully!");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n[ERROR] Setup failed: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
                }
                Console.ResetColor();
                Environment.Exit(1);
            }
        }

        private static async Task CreateQueueAsync(ServiceBusAdministrationClient adminClient)
        {
            Console.WriteLine($"\nChecking Queue '{QueueName}'...");
            bool exists = await adminClient.QueueExistsAsync(QueueName);
            if (exists)
            {
                Console.WriteLine($"Queue '{QueueName}' already exists.");
                return;
            }

            Console.WriteLine($"Creating Queue '{QueueName}'...");
            var options = new CreateQueueOptions(QueueName)
            {
                MaxDeliveryCount = 3,
                LockDuration = TimeSpan.FromSeconds(60),
                DefaultMessageTimeToLive = TimeSpan.FromHours(24),
                DeadLetteringOnMessageExpiration = true
            };

            await adminClient.CreateQueueAsync(options);
            Console.WriteLine($"Queue '{QueueName}' created successfully.");
        }

        private static async Task CreateTopicAsync(ServiceBusAdministrationClient adminClient)
        {
            Console.WriteLine($"\nChecking Topic '{TopicName}'...");
            bool exists = await adminClient.TopicExistsAsync(TopicName);
            if (exists)
            {
                Console.WriteLine($"Topic '{TopicName}' already exists.");
                return;
            }

            Console.WriteLine($"Creating Topic '{TopicName}'...");
            await adminClient.CreateTopicAsync(TopicName);
            Console.WriteLine($"Topic '{TopicName}' created successfully.");
        }

        private static async Task CreateSubscriptionWithFilterAsync(
            ServiceBusAdministrationClient adminClient, 
            string subscriptionName, 
            string sqlExpression)
        {
            Console.WriteLine($"\nChecking Subscription '{subscriptionName}' on Topic '{TopicName}'...");
            bool exists = await adminClient.SubscriptionExistsAsync(TopicName, subscriptionName);

            if (!exists)
            {
                Console.WriteLine($"Creating Subscription '{subscriptionName}'...");
                await adminClient.CreateSubscriptionAsync(TopicName, subscriptionName);
                Console.WriteLine($"Subscription '{subscriptionName}' created.");
            }
            else
            {
                Console.WriteLine($"Subscription '{subscriptionName}' already exists.");
            }

            // Standard rules management
            // Azure Service Bus subscriptions are created with a default "$Default" rule (1=1).
            // If we have a specific SQL filter (not 1=1), we need to replace the default rule.
            if (sqlExpression != "1=1")
            {
                Console.WriteLine($"Applying rule '{sqlExpression}' to '{subscriptionName}'...");
                try
                {
                    // Check if default rule exists and remove it
                    if (await adminClient.RuleExistsAsync(TopicName, subscriptionName, CreateRuleOptions.DefaultRuleName))
                    {
                        await adminClient.DeleteRuleAsync(TopicName, subscriptionName, CreateRuleOptions.DefaultRuleName);
                    }

                    // Add custom filter rule
                    string ruleName = $"{subscriptionName}-filter-rule";
                    if (await adminClient.RuleExistsAsync(TopicName, subscriptionName, ruleName))
                    {
                        await adminClient.DeleteRuleAsync(TopicName, subscriptionName, ruleName);
                    }

                    await adminClient.CreateRuleAsync(TopicName, subscriptionName, new CreateRuleOptions
                    {
                        Name = ruleName,
                        Filter = new SqlRuleFilter(sqlExpression)
                    });

                    Console.WriteLine($"Rule '{ruleName}' ('{sqlExpression}') applied successfully.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to apply rule: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine($"Keeping default rule for '{subscriptionName}' (receives all events).");
            }
        }
    }
}
