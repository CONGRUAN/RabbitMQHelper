using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQDemo
{
    public class Producter
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

        private static SortedSet<ulong> confirmSet = new SortedSet<ulong>();

        public static void DirectExchangeSendMsg()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.ExchangeDeclare(ExchangeName , "direct", durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(QueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);

                    var props = channel.CreateBasicProperties();
                    channel.ConfirmSelect();//设置开启confirm模式
                    props.Persistent = true;
                    #region 异步confirm，批量发送成功后只返回一个ack，所以只执行一次
                    channel.BasicAcks += (model, args) =>
                    {
                        if (args.Multiple)
                        {
                            var tag = args.DeliveryTag + 1UL;
                            confirmSet.Add(tag);
                            confirmSet.RemoveWhere(x => x < tag);
                        }
                        else
                            confirmSet.Remove(args.DeliveryTag);
                    };
                    channel.BasicNacks += (model, args) =>
                    {
                        Console.WriteLine($"Nack,SeqNo:{args.DeliveryTag},Multiple:{args.Multiple}");
                        if (args.Multiple)
                        {
                            var tag = args.DeliveryTag + 1UL;
                            confirmSet.Add(tag);
                            confirmSet.RemoveWhere(x => x < tag);
                        }
                        else
                            confirmSet.Remove(args.DeliveryTag);
                    };
                    #endregion
                    /*string vadata = Console.ReadLine();
                    while(vadata != "exit")
                    {
                        var msgBody = Encoding.UTF8.GetBytes(vadata);
                        channel.BasicPublish(exchange: ExchangeName, routingKey: QueueName, basicProperties: props, body: msgBody);
                        var isConfirm = channel.WaitForConfirms();//WaitForConfirms()方法是同步的，如果用WaitForConfirmsOrDie()将导致程序阻塞
                        if (isConfirm)
                            Console.WriteLine(string.Format("***发送时间:{0}，发送完成，输入exit退出消息发送", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));
                        vadata = Console.ReadLine();
                    }*/
                    for (int i = 0;i < 5;i++)
                    {
                        ulong nextSeqNo = channel.NextPublishSeqNo;
                        var msgBody = Encoding.UTF8.GetBytes($"confirm模式，第{i}条消息");
                        channel.BasicPublish(exchange: ExchangeName, routingKey: QueueName, basicProperties: props, body: msgBody);
                        confirmSet.Add(nextSeqNo);
                    }
                    Console.ReadKey();
                }
            }

        }
    }
}
