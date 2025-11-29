using Confluent.Kafka;

namespace KafkaConsumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("-----------------------------");
            Console.WriteLine("Apache Kafka Consumer in .NET");
            Console.WriteLine("-----------------------------\n");


            // The configuration for the Kafka consumer
            // The 'ConsumerConfig' class is part of the Confluent.Kafka library
            var config = new ConsumerConfig
            {
                GroupId = "test-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            // Create the Kafka consumer
            // Generic types: <Ignore, string> means the key is ignored and the value is a string
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            // Subscribe to the topic named "test-topic"
            consumer.Subscribe("test-topic");

            Console.WriteLine("Consuming messages...");
            while (true)
            {
                var cr = consumer.Consume();
                Console.WriteLine($"Consumed message: {cr.Message.Value} at: {cr.TopicPartitionOffset}");
            }
        }
    }
}
