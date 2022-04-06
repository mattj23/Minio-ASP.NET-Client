using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Minio;

namespace MinioSC
{
    /// <summary>
    /// A client for an external Minio service.  Wraps the raw MinioClient from their nuget library, adding some features
    /// which I found myself continuously having to re-implement.
    /// </summary>
    public class MinioServiceClient
    {
        private readonly Config _config;

        public MinioServiceClient(Config config)
        {
            _config = config;
        }

        public string Bucket => _config.Bucket;

        public MinioClient GetClient()
        {
            return new MinioClient(_config.Host, _config.AccessKey, _config.SecretKey).WithSSL();
        }

        public string FileHash(string fileName)
        {
            using var sha = SHA1.Create();
            using var stream = File.OpenRead(fileName);
            return BitConverter.ToString(sha.ComputeHash(stream)).Replace("-", string.Empty).ToLower();
        }

        public string ByteHash(byte[] bytes)
        {
            using var sha = SHA1.Create();
            return BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-", "").ToLower();
        }

        public async Task<string> HashAsync(Stream stream)
        {
            using var sha = SHA1.Create();
            var result = await sha.ComputeHashAsync(stream);
            return BitConverter.ToString(result).Replace("-", "").ToLower();
        }

        public async Task<byte[]> LoadSmallFileContents(string objectName)
        {
            try
            {
                using var memStream = new MemoryStream();
                var client = GetClient();
                await client.GetObjectAsync(Bucket, objectName, s => s.CopyTo(memStream));

                memStream.Seek(0, SeekOrigin.Begin);
                return memStream.GetBuffer();
            }
            catch (Minio.Exceptions.ObjectNotFoundException)
            {
                return null;
            }
        }

        public Task PutFile(string objectName, Stream stream)
        {
            return GetClient().PutObjectAsync(Bucket, objectName, stream, stream.Length);
        }
        
        public Task PutBytesToFile(string objectName, byte[] bytes)
        {
            using var stream = new MemoryStream(bytes);
            stream.Seek(0, SeekOrigin.Begin);
            return PutFile(objectName, stream);
        }

        public async Task<string> LoadSmallFileText(string objectName)
        {
            var bytes = await LoadSmallFileContents(objectName);
            return bytes == null ? null : Encoding.UTF8.GetString(bytes);
        }

        public async Task DeleteObject(string objectName)
        {
            var client = GetClient();
            await client.RemoveObjectAsync(Bucket, objectName);
        }

        public class Config
        {
            public string Host { get; set; }
            public string AccessKey { get; set; }
            public string SecretKey { get; set; }
            public string Bucket { get; set; }
        }
    }
}