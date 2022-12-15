using KafkaMultiSchemaPoc.BackgroundServices;
using KafkaMultiSchemaPoc.Factories;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<KafkaConsumerFactory>();
builder.Services.AddHostedService<ConsumeTopicsBackgroundService>();

var app = builder.Build();

app.Run();