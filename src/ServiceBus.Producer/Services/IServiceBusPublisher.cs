using System.Threading.Tasks;
using ServiceBus.Contracts.Commands;
using ServiceBus.Contracts.Events;

namespace ServiceBus.Producer.Services
{
    public interface IServiceBusPublisher
    {
        Task SendCommandAsync(ImportMasterDataCommand command);
        Task PublishEventAsync(ImportCompletedEvent completedEvent);
    }
}
