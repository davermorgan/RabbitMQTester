using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{    
    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (sender, ea) =>
    {
        // As this is the retry queue we need to:
        // 1. Check x-retry header value. If this is less than the x-retry-limit header value we retry, otherwise we reject message.
        // 2. If message is rejected it will automatically be placed on dead letter queue
        // 3. If we want to retry the message we will publish it to the main queue, but with modified header values. The x-retry value will be incremented and x-delay value increased exponentially
        
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine($"Retry queue received {message}");

        int retryAttempts = int.Parse(ea.BasicProperties.Headers["x-retry"].ToString());
        int retryLimit = int.Parse(ea.BasicProperties.Headers["x-retry-limit"].ToString());
        int currentDelay = int.Parse(ea.BasicProperties.Headers["x-delay"].ToString());

        if (retryAttempts < retryLimit)
        {
            // retry the message
            Console.WriteLine($"Retrying after {retryAttempts} attempts: {message}");
            Console.WriteLine($"Current delay set to {currentDelay}"); 

            IBasicProperties props = channel.CreateBasicProperties();

            props.Headers = new Dictionary<string, object>();
            props.Headers.Add("x-delay", currentDelay * 3);
            props.Headers.Add("x-retry", retryAttempts + 1);
            props.Headers.Add("x-retry-limit", 5);

            channel.BasicPublish(exchange: "MAIN",
                                 routingKey: "mainQueue",
                                 basicProperties: props,
                                 body: ea.Body);
        }
        else
        {
            // reject the message
            Console.WriteLine($"Sending to dead letter: {message}");
            channel.BasicReject(deliveryTag: ea.DeliveryTag, requeue: false);
        }
    };
    channel.BasicConsume(queue: "retryQueue", autoAck: false, consumer: consumer);
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