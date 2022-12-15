using Avro;
using Avro.Specific;

namespace KafkaMultiSchemaPoc.Deserializers;

public class TopicDataParent<T0, T1> : ISpecificRecord
{

    private int dataType = -1;
    
    public T0 DataTypeOne { get; }
    public T1 DataTypeTwo { get; }

    public TopicDataParent(int dataType, T0 dataTypeOne, T1 dataTypeTwo)
    {
        this.dataType = dataType;
        DataTypeOne = dataTypeOne;
        DataTypeTwo = dataTypeTwo;
        
        if ((dataType != 0 && dataType != 1))
        {
            throw new ArgumentException(
                "A flag do data type deve ser setada para indicar que se o tipo é o primeiro ou segundo");
        }
    }

    public bool IsT0()
    {
        return dataType == 0;
    }
    
    public bool IsT1()
    {
        return dataType == 1;
    }

    public void Match(Action<T0> onT0, Action<T1> onT1)
    {
        if (IsT0())
        {
            onT0?.Invoke(DataTypeOne);
        }
        else
        {
            onT1?.Invoke(DataTypeTwo);
        }
    }

    public object Get(int fieldPos)
    {
        throw new NotImplementedException();
    }

    public void Put(int fieldPos, object fieldValue)
    {
        throw new NotImplementedException();
    }

    public Schema Schema { get; }
}