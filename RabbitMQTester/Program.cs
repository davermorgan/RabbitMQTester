﻿using System;
using RabbitMQ.Client;
using System.Text;

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

    for (int count = 0; count < 10; count++)
    {
        string message = $"Message number {count}";
        var body = Encoding.UTF8.GetBytes(message);

        channel.BasicPublish(exchange: "",
                             routingKey: "mainQueue",
                             basicProperties: null,
                             body: body);
        Console.WriteLine($"Sent {message}");

        Thread.Sleep(100);
    }
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();