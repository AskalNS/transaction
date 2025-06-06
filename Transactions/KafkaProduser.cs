using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions
{
    class KafkaProduser
    {
        static ProducerConfig config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092" // Адрес Kafka брокера
        };

        public static void Send(string message, string topic)
        {

            var config = new ProducerConfig
            {
                BootstrapServers = "kafka:9092"
            };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                producer.Produce(topic, new Message<string, string> { Key = "1", Value = message });
                producer.Flush(TimeSpan.FromSeconds(5));
            }

        }
    }
}
