using System.Threading.Tasks;

namespace ServiceBus.Producer.Services
{
    public interface IBlobStorageService
    {
        Task<string> UploadFileAsync(string fileName, Stream fileStream, string contentType);
        Task<Stream?> DownloadFileAsync(string blobUrl);
    }
}
