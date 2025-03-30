using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Transactions.models;
using Transactions.SharedService;
using WebApplication6.Models;

namespace Transactions
{

    public class KafkaConsumerInvestment
    {
        private readonly string _topic;
        private readonly ConsumerConfig _config;

        public KafkaConsumerInvestment(string topic, string groupId, string bootstrapServers)
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

            InvestmentDTO investmentDTO = JsonConvert.DeserializeObject<InvestmentDTO>(message);
            Console.WriteLine($"Объект: Name = {investmentDTO.Amount}, Age = {investmentDTO.cvv}");


            if (MasterCard.Pay(investmentDTO.number, investmentDTO.date, investmentDTO.cvv, investmentDTO.Amount))
            {
                //using (var db = new MyDbContext())
                //{
                //    db.Investment.Add(new Investment()
                //    {
                //        InvestorId = investmentDTO.InvestorId,
                //        OrderId = investmentDTO.OrderId,
                //        Amount = investmentDTO.Amount,
                //        CreatedAt = DateTimeOffset.Now
                //    });
                //    await Task.Delay(3000);
                //    db.SaveChanges();
                //}
                var response = new InvestmentResponseDTO
                {
                    InvestorId = investmentDTO.InvestorId,
                    OrderId = investmentDTO.OrderId,
                    Amount = investmentDTO.Amount,
                    CreatedAt = DateTimeOffset.Now,
                    result = 1
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "InvestorPaymentResponse");
            }
            else
            {
                var response = new InvestmentResponseDTO
                {
                    InvestorId = investmentDTO.InvestorId,
                    OrderId = investmentDTO.OrderId,
                    Amount = investmentDTO.Amount,
                    CreatedAt = DateTimeOffset.Now,
                    result = 1
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "");
            }



        }
    }
}