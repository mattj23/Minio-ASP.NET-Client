# Minio Client

.NET minio client meant for ASP.NET 

This extremely simple library wraps the basic client from Nuget and implements some things that I kept finding myself having to re-implement, specifically the configuration section in appsettings.json.

## Usage

In the `appsettings.json` file, add a configuration section:

```json
"Minio": {
    "Host": "minio.astinc.services",
    "AccessKey": "cmmc",
    "SecretKey": "EHsQxGwzUEc7eD7VEzLW",
    "Bucket": "cmmc"
  },
```

Then in `Program.cs`:

```csharp
var minioConfig = builder.Configuration.GetSection("Minio").Get<MinioServiceClient.Config>();
builder.Services.AddSingleton(minioConfig);
builder.Services.AddTransient<MinioServiceClient>();
```
