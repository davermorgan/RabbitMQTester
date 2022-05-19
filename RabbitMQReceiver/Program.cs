using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

// See https://medium.com/nmc-techblog/re-routing-messages-with-delay-in-rabbitmq-4a52185f5098
// https://www.cloudamqp.com/blog/when-and-how-to-use-the-rabbitmq-dead-letter-exchange.html

var factory = new ConnectionFactory() { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
   // declareQueues(channel);

    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (sender, ea) =>
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
       // Console.WriteLine("Received {0}", message);

        //int dots = message.Split('.').Length - 1;
        //Thread.Sleep(dots * 1000);

       // Console.WriteLine(" [x] Done {0}", message);

        //int randomNumber = new Random().Next(1, 3);

        //if (randomNumber == 1)
        //{
        //    Console.WriteLine($"Ack {message}");
        //    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        //}
        //else
        //{
            // Fail the message, some transient issue occurred - this would be triggered by an exception in real life but we just do it randomly here!
            Console.WriteLine($"Failed {message}");
            channel.BasicReject(deliveryTag: ea.DeliveryTag , requeue: false);
        //}        
    };
    channel.BasicConsume(queue: "mainQueue", autoAck: false, consumer: consumer);

    //channel.BasicQos(0, 1, false);
    //// declareQueues(channel);

    //var consumer = new EventingBasicConsumer(channel);
    //consumer.Received += (model, ea) =>
    //{
    //    var body = ea.Body.ToArray();
    //    var message = Encoding.UTF8.GetString(body);

    //    // Randomly fail every other message

    //};
    //channel.BasicConsume(queue: "mainQueue",
    //                     autoAck: false,
    //                     consumer: consumer);
    //
    Console.WriteLine(" Press [enter] to exit.");
    Console.ReadLine();
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

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
