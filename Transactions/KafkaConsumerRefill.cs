using System;
using System.Diagnostics;
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
        private static readonly TraceSource Logger = new TraceSource("KafkaConsumerRefill");

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

            // Включение консольного логирования (опционально)
            Trace.Listeners.Add(new ConsoleTraceListener());
        }

        public async Task StartConsuming(CancellationToken cancellationToken)
        {
            await Task.Yield();

            using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
            Logger.TraceEvent(TraceEventType.Information, 0, "Consumer started. Subscribing to topic: " + _topic);
            consumer.Subscribe(_topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Logger.TraceEvent(TraceEventType.Information, 0, $"Received message: {consumeResult.Message.Value} at {consumeResult.TopicPartitionOffset}");

                        await ProcessMessageAsync(consumeResult.Message.Value);

                        consumer.Commit(consumeResult);
                        Logger.TraceEvent(TraceEventType.Information, 0, "Message committed");
                    }
                    catch (OperationCanceledException)
                    {
                        Logger.TraceEvent(TraceEventType.Warning, 0, "Operation canceled");
                        break;
                    }
                    catch (ConsumeException e)
                    {
                        Logger.TraceEvent(TraceEventType.Error, 0, $"Kafka consume error: {e.Error.Reason}");
                    }
                }
            }
            finally
            {
                consumer.Close();
                Logger.TraceEvent(TraceEventType.Information, 0, "Consumer closed");
            }
        }

        private static async Task ProcessMessageAsync(string message)
        {
            try
            {
                await Task.Delay(500);
                Logger.TraceEvent(TraceEventType.Verbose, 0, $"Processing message: {message}");

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

                    Logger.TraceEvent(TraceEventType.Information, 0, $"Refill successful for BusinessId={refillDTO.BusinessId}, Amount={refillDTO.amount}");

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
                    Logger.TraceEvent(TraceEventType.Warning, 0, $"Refill failed for BusinessId={refillDTO.BusinessId}, Amount={refillDTO.amount}");

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
            catch (Exception ex)
            {
                Logger.TraceEvent(TraceEventType.Error, 0, $"Exception while processing message: {ex}");
            }
        }
    }
}
