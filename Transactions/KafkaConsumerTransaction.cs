using Confluent.Kafka;
using Newtonsoft.Json;
using Transactions.models;
using Transactions.SharedService;
using WebApplication6.Models;

namespace Transactions
{
    class KafkaConsumerTransaction
    {
        private readonly string _topic;
        private readonly ConsumerConfig _config;

        public KafkaConsumerTransaction(string topic, string groupId, string bootstrapServers)
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

            TransactionDTO transactionDTO = JsonConvert.DeserializeObject<TransactionDTO>(message);


            if (MasterCard.GetProfit(transactionDTO.cardNumber, transactionDTO.Amount))
            {
                using (var db = new MyDbContext())
                {
                    db.Transaction.Add(new Transaction
                    {
                        InvestorId = transactionDTO.InvestorId,
                        OrderId = transactionDTO.OrderId,
                        Amount = transactionDTO.Amount,
                        CreatedAt = transactionDTO.CreatedAt.UtcDateTime,
                        TrasactionType = transactionDTO.TrasactionType
                    });
                    db.SaveChanges();
                }
                var response = new TransactionDTOResponse
                {
                    InvestorId = transactionDTO.InvestorId,
                    OrderId = transactionDTO.OrderId,
                    Amount = transactionDTO.Amount,
                    CreatedAt = transactionDTO.CreatedAt,
                    TrasactionType = transactionDTO.TrasactionType,
                    Result = 1
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "InvestorTransactionResponse");
            }
            else
            {
                var response = new TransactionDTOResponse
                {
                    InvestorId = transactionDTO.InvestorId,
                    OrderId = transactionDTO.OrderId,
                    Amount = transactionDTO.Amount,
                    CreatedAt = transactionDTO.CreatedAt,
                    TrasactionType = transactionDTO.TrasactionType,
                    Result = 0
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "InvestorTransactionResponse");
            }



        }
    }
}
