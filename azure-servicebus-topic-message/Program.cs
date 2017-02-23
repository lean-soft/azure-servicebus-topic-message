using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.ServiceBus;

namespace azure_servicebus_topic_message
{
    class Program
    {
        public static string BasicTopicName;
        public static string connectionString;
        static void Main(string[] args)
        {
            BasicTopicName = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.BasicTopicName");
            connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var namespaceManager =
                    NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists(BasicTopicName))
            {
                Console.WriteLine("Topic [" + BasicTopicName + "] doesn't exist, create it now!");
                namespaceManager.CreateTopic(BasicTopicName);
            }

            var basicSenario = Activator.CreateInstance(typeof(Basic));
            ((IBasicTopicConnectionStringSample)basicSenario).Run(BasicTopicName, connectionString).GetAwaiter().GetResult();

        }
    }
}
