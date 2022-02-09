using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

// See https://medium.com/nmc-techblog/re-routing-messages-with-delay-in-rabbitmq-4a52185f5098


var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    channel.ExchangeDeclare(exchange: "MAIN",
                            type: "direct",
                            autoDelete: false,
                            durable: true,
                            arguments: null);

    channel.QueueDeclare(queue: "mainQueue",
                         durable: false,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (model, ea) =>
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        // Randomly fail every other message
        int randomNumber = new Random().Next(1,3);

        if (randomNumber == 1)
        {
            channel.BasicAck(ea.DeliveryTag, false);
            Console.WriteLine(" [x] Received {0}", message);
        }
        else
        {
            // Fail the message, some transient issue occurred
            using (var connection2 = factory.CreateConnection())
            using (var channel2 = connection2.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "DELAY",
                                        type: "x-delayed-message",
                                        autoDelete: false,
                                        durable: true,
                                        arguments: new Dictionary<string, object>()
                                        {
                                { "x-delayed-type", "fanout" },
                                { "dead-letter-exchange", "DLX.DEAD.LETTERS" }
                                        });

                channel.QueueDeclare(queue: "retryQueue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                for (int count = 0; count < 10; count++)
                {
                    channel.BasicPublish(exchange: "MAIN",
                                         routingKey: "mainQueue",
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine($"Delayed {message}");

                    Thread.Sleep(100);
                }
            }
        }
    };
    channel.BasicConsume(queue: "mainQueue",
                         autoAck: false,
                         consumer: consumer);
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
