// See https://aka.ms/new-console-template for more information
using Transactions;

Console.WriteLine("Hello, World!");



var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true;
    cts.Cancel();
};

var consumer1 = new KafkaConsumer("InvestorPayment", "consumer-group-1", "localhost:9092");

await Task.WhenAll(
    consumer1.StartConsuming(cts.Token)
);