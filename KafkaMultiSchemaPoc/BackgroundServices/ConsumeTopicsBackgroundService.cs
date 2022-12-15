using br.com.multi_schema.poc;
using Confluent.Kafka;
using KafkaMultiSchemaPoc.Deserializers;
using KafkaMultiSchemaPoc.Factories;

namespace KafkaMultiSchemaPoc.BackgroundServices;

public class ConsumeTopicsBackgroundService : BackgroundService
{
    private readonly KafkaConsumerFactory _consumerFactory;
    private readonly ILogger<KafkaConsumerFactory> _logger;

    public ConsumeTopicsBackgroundService(KafkaConsumerFactory consumerFactory, ILogger<KafkaConsumerFactory> logger)
    {
        _consumerFactory = consumerFactory;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = _consumerFactory.Consumer;
        consumer.Subscribe("topic_multi_schema");

        while (true)
        {
            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

            if (consumeResult is null)
            {
                Task.Delay(TimeSpan.FromSeconds(1));
            }
            else
            {
                ProcessaMensagem(consumeResult.Message);
                Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
        
        return Task.CompletedTask;
    }

    private void ProcessaMensagem(Message<string, TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>> consumeResultMessage)
    {
        consumeResultMessage.Value.Match(
            t0 => _logger.LogInformation("Mensagem consumida: {ConsumedType}", t0.data_type),
            t1 => _logger.LogInformation("Mensagem consumida: {ConsumedType}", t1.data_type));
    }
}