using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

// See https://medium.com/nmc-techblog/re-routing-messages-with-delay-in-rabbitmq-4a52185f5098


var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    channel.QueueDeclare(queue: "mainQueue",
                         durable: false,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);


    channel.ExchangeDeclare(exchange: "DELAY",
                            type: "x-delayed-message",
                            autoDelete: false,
                            durable: true,
                            arguments: new Dictionary<string, object>()
                            {
                                { "x-delayed-type", "fanout" },
                                { "dead-letter-exchange", "DLX.DEAD.LETTERS" }
                            });

    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (model, ea) =>
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine(" [x] Received {0}", message);
    };
    channel.BasicConsume(queue: "mainQueue",
                         autoAck: true,
                         consumer: consumer);

    Console.WriteLine(" Press [enter] to exit.");
    Console.ReadLine();
}