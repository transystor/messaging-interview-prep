using OutboxDemo.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<OutboxPublisherWorker>();

var host = builder.Build();
host.Run();
