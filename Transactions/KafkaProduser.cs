using Confluent.Kafka;
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

        public static void Send(string response)
        {
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                producer.ProduceAsync("my-topic", new Message<Null, string>{Value = response });

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка отправки: {ex.Message}");
            }
        }
    }
}
