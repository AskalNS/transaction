
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Transactions.models;
using Transactions.SharedService;
using WebApplication6.Models;

namespace Transactions
{
    class KafkaConsumerRefill
    {
        private readonly string _topic;
        private readonly ConsumerConfig _config;

        public KafkaConsumerRefill(string topic, string groupId, string bootstrapServers)
        {
            _topic = topic;
            _config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
        }

        public async Task StartConsuming(CancellationToken cancellationToken)
        {
            await Task.Yield();

            using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
            Console.WriteLine("333333333333333333333333333");
            consumer.Subscribe(_topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Console.WriteLine($"Received message: {consumeResult.Message.Value} at {consumeResult.TopicPartitionOffset}");

                        await ProcessMessageAsync(consumeResult.Message.Value);

                        consumer.Commit(consumeResult);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Kafka consume error: {e.Error.Reason}");
                    }
                }
            }
            finally
            {
                consumer.Close();
            }
        }

        private static async Task ProcessMessageAsync(string message)
        {
            await Task.Delay(500);
            Console.WriteLine($"Processed message: {message}");

            RefillDTO refillDTO = JsonConvert.DeserializeObject<RefillDTO>(message);


            if (MasterCard.Pay(refillDTO.number, refillDTO.date, refillDTO.cvv, refillDTO.amount))
            {
                using (var db = new MyDbContext())
                {
                    db.Refill.Add(new Refill
                    {
                        BusinessId = refillDTO.BusinessId,
                        CreatedAt = refillDTO.CreatedAt.UtcDateTime,
                        OrderId = refillDTO.OrderId,
                        Amount = refillDTO.amount
                    });
                    db.SaveChanges();
                }
                var response = new RefillDTOResponse
                {
                    BusinessId = refillDTO.BusinessId,
                    OrderId = refillDTO.OrderId,
                    CreatedAt = refillDTO.CreatedAt,
                    Amount = refillDTO.amount,
                    Result = 1
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "BusinessRefillResponse");
            }
            else
            {
                var response = new RefillDTOResponse
                {
                    BusinessId = refillDTO.BusinessId,
                    OrderId = refillDTO.OrderId,
                    CreatedAt = refillDTO.CreatedAt,
                    Amount = refillDTO.amount,
                    Result = 0
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "BusinessRefillResponse");
            }



        }
    }
}
