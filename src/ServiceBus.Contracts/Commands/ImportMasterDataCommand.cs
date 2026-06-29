using System;

namespace ServiceBus.Contracts.Commands
{
    public record ImportMasterDataCommand
    {
        public string CommandId { get; init; } = Guid.NewGuid().ToString("N");
        public string ImportType { get; init; } = string.Empty; // Country, Port, Vessel, etc.
        public string FileName { get; init; } = string.Empty;
        public string? BlobReference { get; init; } // Optional: Pattern Claim Check (URL blob) - not required for testing
        public long FileSizeBytes { get; init; }
        public int RequestedBy { get; init; } // UserId
        public DateTime RequestedAt { get; init; } = DateTime.UtcNow;
    }
}
