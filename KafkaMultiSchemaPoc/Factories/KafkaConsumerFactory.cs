using Avro.Specific;
using br.com.multi_schema.poc;
using Confluent.Kafka;
using KafkaMultiSchemaPoc.Deserializers;

namespace KafkaMultiSchemaPoc.Factories;

public class KafkaConsumerFactory
{
    private IConsumer<string, TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>> _consumer;
    public IConsumer<string, TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>> Consumer => _consumer;

    public KafkaConsumerFactory()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "multi_schema_poc_app",
            ClientId = "multi_schema_poc_app",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        
        _consumer = new ConsumerBuilder<string, TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>>(consumerConfig)
            .SetValueDeserializer(KafkaMultiSchemaDeserializer.Instance)
            .Build();
    }
    
}