using Confluent.Kafka;
using Newtonsoft.Json;
using Transactions.models;
using Transactions.SharedService;
using WebApplication6.Models;
using Microsoft.Extensions.Logging;

namespace Transactions
{
    class KafkaConsumerTransaction
    {
        private readonly string _topic;
        private readonly ConsumerConfig _config;
        private readonly ILogger<KafkaConsumerTransaction> _logger;

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

            // Логгер создаётся вручную
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            });
            _logger = loggerFactory.CreateLogger<KafkaConsumerTransaction>();
        }

        public async Task StartConsuming(CancellationToken cancellationToken)
        {
            await Task.Yield();

            using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
            _logger.LogInformation("Kafka consumer started and subscribed to topic: {Topic}", _topic);
            consumer.Subscribe(_topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        _logger.LogInformation("Received message: {Message} at {Offset}", consumeResult.Message.Value, consumeResult.TopicPartitionOffset);

                        await ProcessMessageAsync(consumeResult.Message.Value);

                        consumer.Commit(consumeResult);
                        _logger.LogInformation("Message committed.");
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogWarning("Consuming was cancelled.");
                        break;
                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError("Kafka consume error: {Error}", e.Error.Reason);
                    }
                }
            }
            finally
            {
                consumer.Close();
                _logger.LogInformation("Kafka consumer closed.");
            }
        }

        private async Task ProcessMessageAsync(string message)
        {
            try
            {
                await Task.Delay(500); // simulate processing delay
                _logger.LogInformation("Processing message: {Message}", message);

                var transactionDTO = JsonConvert.DeserializeObject<TransactionDTO>(message);

                if (MasterCard.GetProfit(transactionDTO.cardNumber, transactionDTO.Amount))
                {
                    using var db = new MyDbContext();
                    db.Transaction.Add(new Transaction
                    {
                        InvestorId = transactionDTO.InvestorId,
                        OrderId = transactionDTO.OrderId,
                        Amount = transactionDTO.Amount,
                        CreatedAt = transactionDTO.CreatedAt.UtcDateTime,
                        TrasactionType = transactionDTO.TrasactionType
                    });
                    db.SaveChanges();

                    _logger.LogInformation("Transaction saved for OrderId: {OrderId}", transactionDTO.OrderId);

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
                    _logger.LogInformation("Success response sent.");
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
                    _logger.LogWarning("Transaction rejected, response sent.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message: {Message}", message);
            }
        }
    }
}
