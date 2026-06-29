using System.Threading;
using System.Threading.Tasks;
using ServiceBus.Contracts.Commands;
using ServiceBus.Contracts.Events;

namespace ServiceBus.Consumer.Services
{
    public interface IImportProcessor
    {
        Task<ImportCompletedEvent> ProcessImportAsync(ImportMasterDataCommand command, CancellationToken cancellationToken);
    }
}
