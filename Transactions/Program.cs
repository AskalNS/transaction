// See https://aka.ms/new-console-template for more information
using Transactions;

Console.WriteLine("Hello, World!");



var cts = new CancellationTokenSource();
Console.WriteLine("Hello, World!1");
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true;
    cts.Cancel();
};
Console.WriteLine("Hello, World!2");

var consumer1 = new KafkaConsumerInvestment("InvestorPayment", "consumer-group-1", "localhost:9092");
var consumer2 = new KafkaConsumerRefill("BusinessRefill", "consumer-group-1", "localhost:9092");
var consumer3 = new KafkaConsumerTransaction("InvestorTransaction", "consumer-group-1", "localhost:9092");
Console.WriteLine("Hello, World!3");
var thread1 = new Thread(() => consumer1.StartConsuming(cts.Token)) { IsBackground = true };
var thread2 = new Thread(() => consumer2.StartConsuming(cts.Token)) { IsBackground = true };
var thread3 = new Thread(() => consumer3.StartConsuming(cts.Token)) { IsBackground = true };
Console.WriteLine("Hello, World!4");
thread1.Start();
thread2.Start();
thread3.Start();
Console.WriteLine("Hello, World!5");
// Ждем завершения работы
await Task.Run(() => {
    thread1.Join();
    thread2.Join();
    thread3.Join();
});
Console.WriteLine("Hello, World!6");


await Task.Delay(Timeout.Infinite, cts.Token);


