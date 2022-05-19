using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

// See https://medium.com/nmc-techblog/re-routing-messages-with-delay-in-rabbitmq-4a52185f5098


var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (model, ea) =>
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine($"Retry queue received {message}");
    };
    channel.BasicConsume(queue: "retryQueue",
                         autoAck: true,
                         consumer: consumer);
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

/// <summary>
/// Determine if we should retry, depending on what we get from the headers
/// </summary>
bool ShouldRetry(string headers)
{
    return true;
}

void declareQueues(IModel channel)
{
    // Declare exchanges and queues

    // Main exchange uses DELAY as it's dead letter exchange
    channel.ExchangeDeclare(exchange: "MAIN",
                            type: "direct",
                            autoDelete: false,
                            durable: true,
                            arguments: new Dictionary<string, object>()
                            {
                                { "dead-letter-exchange", "DELAY" }
                            });

    channel.ExchangeDeclare(exchange: "DELAY",
                    type: "x-delayed-message",
                    autoDelete: false,
                    durable: true,
                    arguments: new Dictionary<string, object>()
                    {
                                { "x-delayed-type", "fanout" },
                                { "dead-letter-exchange", "DLX.DEAD.LETTERS" }
                    });

    channel.ExchangeDeclare(exchange: "DLX.DEAD.LETTERS",
                    type: "direct",
                    autoDelete: false,
                    durable: true,
                    arguments: null);

    channel.QueueDeclare(queue: "mainQueue",
                         durable: true,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    channel.QueueBind("mainQueue", "MAIN", "mainQueue", null);

    channel.QueueDeclare(queue: "retryQueue",
                         durable: true,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    channel.QueueBind("retryQueue", "DELAY", "retryQueue", null);

    channel.QueueDeclare(queue: "dlQueue",
                         durable: true,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    channel.QueueBind("dlQueue", "DLX.DEAD.LETTERS", "dlQueue", null);
}