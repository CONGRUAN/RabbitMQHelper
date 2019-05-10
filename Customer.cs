using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQReciever
{
    public class Customer
    {
        /// <summary>
        /// 连接配置
        /// </summary>
        private static readonly ConnectionFactory rabbitMqFactory = new ConnectionFactory()
        {
            HostName = "127.0.0.1",
            UserName = "guest",
            Password = "guest",
            Port = 5672
        };
        /// <summary>
        /// 路由名称
        /// </summary>
        const string ExchangeName = "justin.exchange";

        /// <summary>
        /// 队列名称
        /// </summary>
        const string QueueName = "justin.queue";
        /// <summary>
        /// 该方式不建议使用，处理速度较慢，循环线程等待
        /// </summary>
        public static void DirectAcceptExchange()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.ExchangeDeclare(ExchangeName, "direct", durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(QueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);
                    while (true)
                    {
                        BasicGetResult msgResponse = channel.BasicGet(QueueName, true);
                        if (msgResponse != null)
                        {
                            var msgBody = Encoding.UTF8.GetString(msgResponse.Body);
                            Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                        }
                        
                        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1));
                    }
                }
            }
        }
        /// <summary>
        /// 使用事件高效接收消息
        /// 缺点：分配过来的消息过多的时候，消费者同一时间没有能力处理，压力较大，导致分配过来的队列消息不能及时处理完成
        /// </summary>
        public static void DirectAcceptExchangeEvent()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    //路由也可以不定义，使用默认的
                    //channel.ExchangeDeclare(ExchangeName, "direct", durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(QueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    //channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var msgBody = Encoding.UTF8.GetString(ea.Body);
                        Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                    };
                    //自动确认，如果异常的话消息就丢失了
                    channel.BasicConsume(QueueName, true, consumer: consumer);
                    
                    Console.WriteLine("按任意值，退出程序");
                    Console.ReadKey();
                }
            }
        }
        /// <summary>
        /// 控制流量来接收消息，自定义同一时间接收的最大消息数，避免一次性被分配过多消息
        /// rabbitmq分配消息原理：rabbitmq将队列中的一条消息投递给消费者时，会遍历该队列上的消费者，选择一个合适的消费者投递出去。
        /// 挑选的消费者条件：该消费者对应的channel上未ack的消息数是否达到设置的prefetch_count个数，如果未ack的消息数达到了prefetch_count的个数，则不符合要求
        /// </summary>
        public static void DirectAcceptExchangeTask()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.QueueDeclare(QueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);//控制接收流量，告诉broker同一时间只处理一个消息
                                                                                       
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var msgBody = Encoding.UTF8.GetString(ea.Body);
                        try
                        {
                            Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                            //处理完成，告诉Broker可以服务端可以删除消息，分配新的消息过来
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                        catch (Exception)
                        {
                            //异常的话重新处理
                        }
                    };
                    //手动确认，autoAck设置false,告诉broker，发送消息之后，消息暂时不要删除，等消费者处理完成再说
                    channel.BasicConsume(QueueName, false, consumer: consumer);

                    Console.WriteLine("按任意值，退出程序");
                    Console.ReadKey();
                }
            }
        }
    }
}
