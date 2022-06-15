using System;
using RabbitMQ.Client;
using System.Text;

var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    // Declare exchanges and queues
    declareQueues(channel);

    for (int count = 0; count < 1000; count++)
    {
        string message = $"Message number {count}";
        var body = Encoding.UTF8.GetBytes(message);

        // Create basic props and add headers to set the retry settings for a new message. These will be modified as the message gets retried, should it fail
        IBasicProperties props = channel.CreateBasicProperties();

        props.Headers = new Dictionary<string, object>();
        props.Headers.Add("x-delay", 1000);
        props.Headers.Add("x-retry", 0);
        props.Headers.Add("x-retry-limit", 5);

        channel.BasicPublish(exchange: "MAIN",
                             routingKey: "mainQueue",
                             basicProperties: props,
                             body: body);
        Console.WriteLine($"Sent {message}");
    }
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

void declareQueues(IModel channel)
{
    channel.CreateBasicProperties();
    // Declare exchanges and queues

    // Main exchange uses DELAY as it's dead letter exchange
    channel.ExchangeDeclare(exchange: "MAIN",
                            type: "direct",
                            autoDelete: false,
                            durable: true,
                            arguments: null);

    channel.ExchangeDeclare(exchange: "DELAY",
                    type: "x-delayed-message",
                    autoDelete: false,
                    durable: true,
                    arguments: new Dictionary<string, object>()
                    {
                                { "x-delayed-type", "direct" }
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
                         arguments: new Dictionary<string, object>()
                            {
                                { "x-dead-letter-exchange", "DELAY" },
                                { "x-dead-letter-routing-key", "retryQueue" }
                            });

    channel.QueueBind("mainQueue", "MAIN", "mainQueue", null);

    channel.QueueDeclare(queue: "retryQueue",
                         durable: true,
                         exclusive: false,
                         autoDelete: false,
                         arguments: new Dictionary<string, object>()
                    {
                                { "x-delayed-type", "direct" },
                                { "x-dead-letter-exchange", "DLX.DEAD.LETTERS" },
                                { "x-dead-letter-routing-key", "dlQueue" }
                    });

    channel.QueueBind("retryQueue", "DELAY", "retryQueue", null);

    channel.QueueDeclare(queue: "dlQueue",
                         durable: true,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    channel.QueueBind("dlQueue", "DLX.DEAD.LETTERS", "dlQueue", null);
}