using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Transactions.models;
using Transactions.SharedService;

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
            Console.WriteLine($"Объект: Name = {investmentDTO.amount}, Age = {investmentDTO.cvv}");


            if (MasterCard.Pay(investmentDTO.number, investmentDTO.date, investmentDTO.cvv, investmentDTO.amount))
            {
                using (var db = new MyDbContext())
                {
                    db.Investment.Add(new Investment
                    {
                        InvestorId = investmentDTO.InvestorId,
                        InvestorFio = investmentDTO.InvestorFio,
                        InvestorIin = investmentDTO.InvestorIin,
                        BusinessId = investmentDTO.BusinessId,
                        BusinessFio = investmentDTO.BusinessFio,
                        BusinessIin = investmentDTO.BusinessBin,
                        Amount = investmentDTO.amount
                    });
                    db.SaveChanges();
                }
                var response = new InvestmentResponseDTO
                {
                    id = investmentDTO.id,
                    result = 1011
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "InvestorPaymentResponse");
            }
            else
            {
                var response = new InvestmentResponseDTO
                {
                    id = investmentDTO.id,
                    InvestorId = investmentDTO.InvestorId,
                    InvestorFio = investmentDTO.InvestorFio,
                    InvestorIin = investmentDTO.InvestorIin,
                    BusinessId = investmentDTO.BusinessId,
                    BusinessFio = investmentDTO.BusinessFio,
                    BusinessBin = investmentDTO.BusinessBin,
                    Amount = investmentDTO.amount,
                    result = 1011
                };
                string json = JsonConvert.SerializeObject(response);
                KafkaProduser.Send(json, "");
            }


            
        }
    }
}