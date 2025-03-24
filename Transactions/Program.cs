// See https://aka.ms/new-console-template for more information
using Transactions;

Console.WriteLine("Hello, World!");



var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true;
    cts.Cancel();
};

var consumer1 = new KafkaConsumerInvestment("InvestorPayment", "consumer-group-1", "localhost:9092");
var consumer2 = new KafkaConsumerRefill("BusinessRefill", "consumer-group-1", "localhost:9092");
var consumer3 = new KafkaConsumerTransaction("InvestorTransaction", "consumer-group-1", "localhost:9092");

await Task.WhenAll(
    consumer1.StartConsuming(cts.Token)
);