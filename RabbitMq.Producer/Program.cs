using System;
using System.Text;
using RabbitMQ.Client;

namespace RabbitMq.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", Port = 32785 };

            while (true)
            {
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "process.queue",
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    for (int i = 0; i < 1000; i++)
                    {
                        string message = $"Hello World!! {i}";
                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: "",
                                             routingKey: "process.queue",
                                             basicProperties: null,
                                             body: body);
                        Console.WriteLine(" [x] Sent {0}", message);
                    }
                }

                Console.WriteLine(" Press [enter] to repeat.");
                Console.ReadLine();
            }
        }
    }
}
