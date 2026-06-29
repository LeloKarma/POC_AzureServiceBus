using System;

namespace ServiceBus.Contracts.Events
{
    public record ImportCompletedEvent
    {
        public string CommandId { get; init; } = string.Empty;
        public string ImportType { get; init; } = string.Empty;
        public string Status { get; init; } = string.Empty; // "Completed" | "Failed"
        public int TotalRowsProcessed { get; init; }
        public int ErrorCount { get; init; }
        public string? ErrorMessage { get; init; }
        public DateTime CompletedAt { get; init; } = DateTime.UtcNow;
        public TimeSpan Duration { get; init; }
        public int RetryCount { get; init; } = 0; // Track retry attempts
    }
}
