using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using System.Text;
using System.Text.Json;

namespace SBSender
{
    class Program
    {
        const string version = "0.0.1";

        class Config
        {
            public int count { get; set; }
            public string msgprefix { get; set; }
            public string queueName { get; set; }
            public string connectionString { get; set; }


            public Config()
            {
                this.count = 5;
                this.msgprefix = "";
                this.queueName = "";
                this.connectionString = "";
            }

            public void ReadConfigFile(string configPath = @"./sender.config.json")
            {
                try
                {
                    using (StreamReader st = new StreamReader(configPath, Encoding.GetEncoding("UTF-8")))
                    {
                        string configJson = st.ReadToEnd();
                        Config readConfig = JsonSerializer.Deserialize<Config>(configJson);
                        this.count = readConfig.count;
                        this.msgprefix = readConfig.msgprefix;
                        this.queueName = readConfig.queueName;
                        this.connectionString = readConfig.connectionString;

                    }
                }
                catch (IOException e)
                {
                    Console.WriteLine("Failed to read config file.");
                    Console.WriteLine(e.Message);
                }
            }

            public void ReadCmdArgs(string[] cmdArgs)
            {
                for (int i = 1; i < cmdArgs.Length; i++)
                {
                    //Console.WriteLine("command : {0:d} : {1}\r\n", i, Commands[i]);
                    switch (cmdArgs[i])
                    {
                        // the number of messages sent to ServiceBus
                        case "-c":
                            this.count = int.Parse(cmdArgs[i + 1]);
                            break;
                        case "--count":
                            this.count = int.Parse(cmdArgs[i + 1]);
                            break;

                        // message prefix
                        case "-p":
                            this.msgprefix = cmdArgs[i + 1];
                            break;
                        case "--prefix":
                            this.msgprefix = cmdArgs[i + 1];
                            break;

                        // ServiceBus Queue name for the destination
                        case "-n":
                            this.queueName = cmdArgs[i + 1];
                            break;
                        case "--name":
                            this.queueName = cmdArgs[i + 1];
                            break;

                        // connection string of ServiceBus
                        case "-s":
                            this.connectionString = cmdArgs[i + 1];
                            break;
                        case "--connectionstring":
                            this.connectionString = cmdArgs[i + 1];
                            break;
                    }
                }
            }

            public void PrintValue()
            {
                Console.WriteLine("count \t\t\t: {0}", count);
                Console.WriteLine("msgprefix \t\t: {0}", msgprefix);
                Console.WriteLine("queueName \t\t: {0}", queueName);
                Console.WriteLine("connectionString \t: {0}", connectionString);
            }
        }

        static async Task Main(string[] args)
        {

            string[] Commands = System.Environment.GetCommandLineArgs();

            // Configration initialize
            Config config = new Config();

            if (Commands.Length < 2)
            {
                DisplayAbstract();
                return;
            }
            else
            {
                // Read configration value from ./config.json
                config.ReadConfigFile();

                // Read configration values from ./sender.config.json
                config.ReadCmdArgs(Commands);
            }


            if (config.connectionString != "")
            {
                Console.WriteLine("-----------------");
                config.PrintValue();
                Console.WriteLine("-----------------");
                await SendMessageBatchAsync(config);
            }
           
        }

        static async Task SendMessageAsync(Config config)
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(config.connectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(config.queueName);

                // create a message that we can send
                ServiceBusMessage message = new ServiceBusMessage("Hello world!");

                // send the message
                await sender.SendMessageAsync(message);
                Console.WriteLine($"Sent a single message to the queue: {config.queueName}");
            }
        }

        static Queue<ServiceBusMessage> CreateMessages(int count, string prefix)
        {
            // create a queue containing the messages and return it to the caller
            Queue<ServiceBusMessage> messages = new Queue<ServiceBusMessage>();

            for (int i = 0; i < count; i++)
            {
                string msg = prefix + " msg " + (i + 1) + "/" + count + " " + DateTime.Now.ToString();
                Console.WriteLine(msg);
                messages.Enqueue(new ServiceBusMessage(msg));
            }

            return messages;
        }

        static async Task SendMessageBatchAsync(Config config)
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(config.connectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(config.queueName);

                // get the messages to be sent to the Service Bus queue
                Queue<ServiceBusMessage> messages = CreateMessages(config.count, config.msgprefix);

                // total number of messages to be sent to the Service Bus queue
                int messageCount = messages.Count;

                // while all messages are not sent to the Service Bus queue
                while (messages.Count > 0)
                {
                    // start a new batch 
                    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                    // add the first message to the batch
                    if (messageBatch.TryAddMessage(messages.Peek()))
                    {
                        // dequeue the message from the .NET queue once the message is added to the batch
                        messages.Dequeue();
                    }
                    else
                    {
                        // if the first message can't fit, then it is too large for the batch
                        throw new Exception($"Message {messageCount - messages.Count} is too large and cannot be sent.");
                    }

                    // add as many messages as possible to the current batch
                    while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
                    {
                        // dequeue the message from the .NET queue as it has been added to the batch
                        messages.Dequeue();
                    }

                    // now, send the batch
                    await sender.SendMessagesAsync(messageBatch);

                    // if there are any remaining messages in the .NET queue, the while loop repeats 
                }

                Console.WriteLine($"Sent a batch of {messageCount} messages to the topic: {config.queueName}");
            }
        }

        static void DisplayAbstract()
        {
            Console.WriteLine(Figgle.FiggleFonts.Standard.Render("ServiceBus Sender!"));
            Console.WriteLine("ServiceBusSender v{0} - simple messages sending utility to ServiceBus", version);
            Console.WriteLine("Copyright (C) Yusuke.Yoneda");
            Console.WriteLine("");
            Console.WriteLine("ServiceBusSender usage: sbsend [-c/--count MessageCount] [-p/--prefix MessagePrefix] [-n/--name QueueName] [-s/--connectionstring ConnectionString]");
            Console.WriteLine(" -c\tthe number of messages sent to ServiceBus Queue. default value is 5.");
            Console.WriteLine(" -p\tmessage prefix. If Prefix is \"hoge\", the mssage is \"hoge messages #{num} yyyy/mm/dd hh:mm:ss");
            Console.WriteLine(" -n\tqueue name for the destination");
            Console.WriteLine(" -s\tconnection string of ServiceBus");
        }

    }
}
