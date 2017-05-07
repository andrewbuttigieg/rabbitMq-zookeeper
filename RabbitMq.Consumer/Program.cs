using System;
using System.Text;
using System.Threading.Tasks;
using org.apache.zookeeper;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using static org.apache.zookeeper.ZooDefs;

namespace RabbitMq.Consumer
{
    class Program
    {
        static ZooKeeper zooKeeper = null;


		private static void SyncPrimitive(string address)
		{
			if (zooKeeper == null)
			{
				try
				{
					Console.WriteLine("Starting ZK:");
					zooKeeper = new ZooKeeper(address, 3000, null);
					//mutex = new Integer(-1);
					Console.WriteLine("Finished starting ZK: " + zooKeeper);
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					zooKeeper = null;
				}
			}
			//else mutex = new Integer(-1);
		}

        static void Main(string[] args)
        {
            SyncPrimitive("localhost:32790");

			var factory = new ConnectionFactory() { HostName = "localhost", Port = 32785 };
			using (var connection = factory.CreateConnection())
			using (var channel = connection.CreateModel())
			{
				channel.QueueDeclare(queue: "process.queue",
									 durable: true,
									 exclusive: false,
									 autoDelete: false,
									 arguments: null);

				var consumer = new EventingBasicConsumer(channel);
				consumer.Received += (model, ea) =>
				{
					string name = $"/{ea.Body}";
                    try
                    {
                        //zooKeeper.deleteAsync(name).Wait();
                        var task = zooKeeper.createAsync(name, 
                                              Encoding.ASCII.GetBytes(name), 
                                              Ids.OPEN_ACL_UNSAFE, 
                                                         CreateMode.EPHEMERAL_SEQUENTIAL);
                        task.Wait();

                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);

                        zooKeeper.deleteAsync(name);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(name);
                    }
                 };
				channel.BasicConsume(queue: "process.queue",
									 //noAck: true,
									 consumer: consumer);

				Console.WriteLine(" Press [enter] to exit.");
				Console.ReadLine();
			}
        }

    }
}
