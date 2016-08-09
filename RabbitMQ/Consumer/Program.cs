using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            string hostName = "192.168.8.194";
            string userName = "sys";
            string password = "123456";
            string queueName = "Message-Queue";

            const int NUMBER_OF_WORKROLES = 3;

            //Cria a conexão com o RabbitMq
            var factory = new ConnectionFactory()
            {
                HostName = hostName,
                UserName = userName,
                Password = password,
            };

            //Cria a conexão
            IConnection connection = factory.CreateConnection();

            //cria a canal de comunicação com a rabbit mq
            IModel channel = connection.CreateModel();

            for (int i = 0; i < NUMBER_OF_WORKROLES; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    lock (channel)
                    {

                        var consumer = new EventingBasicConsumer(channel);
                        consumer.ConsumerTag = Guid.NewGuid().ToString(); // Tag de identificação do consumidor no RabbitMQ
                        consumer.Received += (sender, ea) =>
                        {
                            var body = ea.Body;
                            var brokerMessage = Encoding.Default.GetString(ea.Body);

                            Console.WriteLine($"Mensagem recebida com o valor: {brokerMessage}");
                            
                            //Diz ao RabbitMQ que a mensagem foi lida com sucesso pelo consumidor
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: true);
                        };

                        //Registra os consumidor no RabbitMQ
                        channel.BasicConsume(queueName, noAck: false, consumer: consumer);

                    }
                });
            }

            Console.Read();
        }
    }
}
