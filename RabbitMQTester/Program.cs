using System;
using RabbitMQ.Client;
using System.Text;

var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    channel.ExchangeDeclare(exchange: "MAIN",
                            type: "direct",
                            autoDelete: false,
                            durable: true,
                            arguments: new Dictionary<string, object>()
                            {
                                { "x-delayed-type", "fanout" },
                                { "dead-letter-exchange", "DELAY" }
                            });

    channel.QueueDeclare(queue: "mainQueue",
                         durable: false,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    for (int count = 0; count < 10; count++)
    {
        string message = $"Message number {count}";
        var body = Encoding.UTF8.GetBytes(message);

        channel.BasicPublish(exchange: "MAIN",
                             routingKey: "mainQueue",
                             basicProperties: null,
                             body: body);
        Console.WriteLine($"Sent {message}");

        Thread.Sleep(100);
    }
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();