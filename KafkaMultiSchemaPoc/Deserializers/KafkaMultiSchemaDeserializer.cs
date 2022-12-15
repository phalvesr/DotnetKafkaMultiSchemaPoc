using System.Text;
using System.Text.Json;
using br.com.multi_schema.poc;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaMultiSchemaPoc.Deserializers;

// O codigo comentado mostra como fariamos para consumir de um schema registry
public class KafkaMultiSchemaDeserializer : IDeserializer<TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>>
{
    public static KafkaMultiSchemaDeserializer Instance = new KafkaMultiSchemaDeserializer();
    
    // private IDeserializer<TopicDataTypeOne> _deserializerTopicDataTypeOne = new AvroDeserializer<TopicDataTypeOne>(new CachedSchemaRegistryClient(new SchemaRegistryConfig
    // {
    //     
    // })).AsSyncOverAsync();
    // private IDeserializer<TopicDataTypeTwo> _deserializerTopicDataTypeTwo = new AvroDeserializer<TopicDataTypeTwo>(new CachedSchemaRegistryClient(new SchemaRegistryConfig
    // {
    //     
    // })).AsSyncOverAsync();

    public TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            return default;
        }
        
        var typeHeader = context.Headers.FirstOrDefault(header => header.Key == "type");

        if (typeHeader is null)
        {
            // Poderiamos fazer uma tratativa para enviar para um DLQ
            throw new InvalidCastException("Sem tipo no header");
        }
        
        var typeAsString = Encoding.UTF8.GetString(typeHeader.GetValueBytes());
        
        // return typeAsString switch
        // {
        //     "TopicDataTypeOne" => new TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>(0, _deserializerTopicDataTypeOne.Deserialize(data, isNull, context), default),
        //     "TopicDataTypeTwo" => new TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>(0, default,_deserializerTopicDataTypeTwo.Deserialize(data, isNull, context)),
        //     _ => throw new InvalidCastException("Nao deveria acontecer")
        // };

        return typeAsString switch
        {
            nameof(TopicDataTypeOne) => new TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>(0, JsonSerializer.Deserialize<TopicDataTypeOne>(data), default),
            nameof(TopicDataTypeTwo) => new TopicDataParent<TopicDataTypeOne, TopicDataTypeTwo>(1, default, JsonSerializer.Deserialize<TopicDataTypeTwo>(data)),
            _ => throw new InvalidCastException("Nao deveria acontecer")
        };
    }
}
