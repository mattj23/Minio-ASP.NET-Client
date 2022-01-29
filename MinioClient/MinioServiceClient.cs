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
        private readonly string _host;
        private readonly string _accessKey;
        private readonly string _secretKey;

        public MinioServiceClient(string connectionString)
        {
            var parts = connectionString.Split(';');
            if (parts.Length < 4)
            {
                throw new ArgumentException("Minio connection string needs four semicolon-separated values");
            }

            _host = parts[0];
            _accessKey = parts[1];
            _secretKey = parts[2];
            Bucket = parts[3];
        }

        public string Bucket { get; }

        public MinioClient GetClient()
        {
            return new MinioClient(_host, _accessKey, _secretKey).WithSSL();
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

        public Task WriteBytesToFile(string objectName, byte[] bytes)
        {
            using var stream = new MemoryStream(bytes);
            stream.Seek(0, SeekOrigin.Begin);
            return GetClient().PutObjectAsync(Bucket, objectName, stream, stream.Length);
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

    }
}