using System.Threading.Tasks;

namespace ServiceBus.Consumer.Services
{
    public interface IBlobStorageService
    {
        Task<Stream?> DownloadFileAsync(string blobUrl);
    }
}
