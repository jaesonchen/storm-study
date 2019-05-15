/**   
 * storm:
Storm是一个分布式实时计算系统。利用Storm可以很容易做到可靠地处理无限的数据流，像Hadoop批量处理大数据一样，Storm可以实时处理数据。
Storm是为分布式场景而生的，抽象了消息传递，会自动地在集群机器上并发地处理流式计算，让你专注于实时处理的业务逻辑。
Storm上构建的拓扑处理的是持续不断的流式数据。不同于Hadoop的任务，这些处理过程不会终止，会持续处理到达的数据。
Storm处理的是动态的、连续的数据，而Hadoop处理的是静态的数据。
Storm不仅仅是一个传统的大数据分析系统：它是一个复杂事件(complex event-processing)处理系统的例子。
     复杂事件处理系统通常是面向检测和计算的，这两部分都可以通过用户定义的算法在Storm中实现。

Storm的特点
1. 编程简单：开发人员只需要关注应用逻辑，而且跟Hadoop类似，Storm提供的编程原语也很简单
2. 高性能，低延迟：可以应用于广告搜索引擎这种要求对广告主的操作进行实时响应的场景。
3. 分布式：可以轻松应对数据量大，单机搞不定的场景
4. 可扩展： 随着业务发展，数据量和计算量越来越大，系统可水平扩展
5. 容错：单个节点挂了不影响应用
6. 消息不丢失：保证消息处理

Storm不是一个完整的解决方案。使用Storm时你需要关注以下几点：
1. 如果使用的是自己的消息队列，需要加入消息队列做数据的来源和产出的代码
2. 需要考虑如何做故障处理：如何记录消息队列处理的进度，应对Storm重启，挂掉的场景
3. 需要考虑如何做消息的回退：如果某些消息处理一直失败怎么办？

Storm的应用
Storm处理速度很快：每个节点每秒钟可以处理超过百万的数据组。它是可扩展(scalable)，容错(fault-tolerant)，保证你的数据会被处理，并且很容易搭建和操作。
跟Hadoop不一样，Storm是没有包括任何存储概念的计算系统。这就让Storm可以用在多种不同的场景下：
实时分析(realtime analysis)
在线机器学习(online machine learning)
连续计算(continuous computation)
分布式远程过程调用(RPC)
ETL


Storm模型
Storm实现了一个数据流(data flow)的模型，在这个模型中数据持续不断地流经一个由很多转换实体构成的网络。
一个数据流的抽象叫做流(stream)，流是无限的元组(Tuple)序列。元组就像一个可以表示标准数据类型（例如int，float和byte数组）
和用户自定义类型（需要额外序列化代码的）的数据结构。每个流由一个唯一的ID来标示的，这个ID可以用来构建拓扑中各个组件的数据源。
Storm对数据输入的来源和输出数据的去向没有做任何限制。在Storm里，可以使用任意来源的数据输入和任意的数据输出，只要你实现对应的代码来获取/写入这些数据就可以。
典型场景下，输入/输出数据来是基于类似Kafka或者ActiveMQ这样的消息队列，但是数据库，文件系统或者web服务也都是可以的。


Storm中涉及的主要概念有：
拓扑(Topology)
一个Storm拓扑打包了一个实时处理程序的逻辑。一个Storm拓扑跟一个MapReduce的任务(job)是类似的。
主要区别是MapReduce任务最终会结束，而拓扑会一直运行（直到你杀死它)。一个拓扑是一个通过流分组(stream grouping)把Spout和Bolt连接到一起的拓扑结构。
图的每条边代表一个Bolt订阅了其他Spout或者Bolt的输出流。一个拓扑就是一个复杂的多阶段的流计算。
TopologyBuilder: 使用这个类来在Java中创建拓扑
运行Topology：集群模式和本地模式(通常用于测试) 

元组(Tuple)
元组是Storm提供的一个轻量级的数据格式，可以用来包装你需要实际处理的数据。元组是一次消息传递的基本单元。
一个元组是一个命名的值列表，其中的每个值都可以是任意类型的。元组是动态地进行类型转化的--字段的类型不需要事先声明。
在Storm中编程时，就是在操作和转换由元组组成的流。通常，元组包含整数，字节，字符串，浮点数，布尔值和字节数组等类型。
要想在元组中使用自定义类型，就需要实现自己的序列化方式。

流(Streams)
流是Storm中的核心抽象。一个流由无限的元组序列组成，这些元组会被分布式并行地创建和处理。通过流中元组包含的字段名称来定义这个流。
OutputFieldsDeclarer: 用来声明流和流的定义
Serialization: Storm元组的动态类型转化，声明自定义的序列化方式
ISerialization: 自定义的序列化必须实现这个接口
CONFIG.TOPOLOGY_SERIALIZATIONS: 可以通过这个配置来注册自定义的序列化接口

Spout
Spout(喷嘴)是Storm中流的来源。通常Spout从外部数据源，如消息队列中读取元组数据并吐到拓扑里。
Spout可以是可靠的(reliable)或者不可靠(unreliable)的。
可靠的Spout能够在一个元组被Storm处理失败时重新进行处理，而非可靠的Spout只是吐数据到拓扑里，不关心处理成功还是失败了。
Spout可以一次给多个流吐数据。此时需要通过OutputFieldsDeclarer的declareStream函数来声明多个流并在调用SpoutOutputCollector提供的emit方法时指定元组吐给哪个流。
Spout中最主要的函数是nextTuple，Storm框架会不断调用它去做元组的轮询。如果没有新的元组过来，就直接返回，否则把新元组吐到拓扑里。
nextTuple必须是非阻塞的，因为Storm在同一个线程里执行Spout的函数。
Spout中另外两个主要的函数是ack和fail。当Storm检测到一个从Spout吐出的元组在拓扑中成功处理完时调用ack,没有成功处理完时调用fail。
只有可靠型的Spout会调用ack和fail函数。

Bolt
在拓扑中所有的计算逻辑都是在Bolt中实现的。一个Bolt可以处理任意数量的输入流，产生任意数量新的输出流。
Bolt可以做函数处理，过滤，流的合并，聚合，存储到数据库/hdfs等操作。
Bolt就是流水线上的一个处理单元，把数据的计算处理过程合理的拆分到多个Bolt、合理设置Bolt的task数量，能够提高Bolt的处理能力，提升流水线的并发度。
Bolt可以给多个流吐出元组数据。此时需要使用OutputFieldsDeclarer的declareStream方法来声明多个流并在调用OutputColletor提供的emit方法时指定给哪个流吐数据。
当你声明了一个Bolt的输入流，也就订阅了另外一个组件的某个特定的输出流。如果希望订阅另一个组件的所有流，需要单独挨个订阅。
InputDeclarer有语法糖来订阅ID为默认值的流。declarer.shuffleGrouping("redBolt")订阅了redBolt组件上的默认流，跟declarer.shuffleGrouping("redBolt", DEFAULT_STREAM_ID)是相同的。
Bolt中最主要的函数是execute函数，它使用一个新的元组当作输入。Bolt使用OutputCollector对象来吐出新的元组。
Bolts必须为处理的每个元组调用OutputCollector的ack方法以便于Storm知道元组什么时候被各个Bolt处理完了（最终就可以确认Spout吐出的某个元组处理完了）。
通常处理一个输入的元组时，会基于这个元组吐出零个或者多个元组，然后确认(ack)输入的元组处理完了，Storm提供了IBasicBolt接口来自动完成确认。
必须注意OutputCollector不是线程安全的，所以所有的吐数据(emit)、确认(ack)、通知失败(fail)必须发生在同一个线程里。
IRichBolt: 这是Bolt的通用接口
IBasicBolt: 很方便的Bolt接口，用于定义做过滤或者简单处理的Bolt
OutputCollector: Bolt通过这个类的实例来吐元组给输出流

IRichBolt和IBasicBolt
使用IBasicBolt/BaseBasicBolt不需要总是调用collect.ack，storm会帮我们处理。
对于spout，有ISpout，IRichSpout，BaseRichSpout
对于bolt，有IBolt，IRichBolt，BaseRichBolt，IBasicBolt，BaseBasicBolt
IBasicBolt，BaseBasicBolt不用每次execute完成都写ack/fail，因为已经帮你实现好了。
发送到BasicOutputCollector的tuple会自动和输入tuple相关联，而在execute方法结束的时候那个输入tuple会被自动ack的。


组件(Component) 是对Bolt和Spout的统称。

Worker(工作进程)
拓扑的每台Supervisor以一个或多个Worker进程的方式运行。每个Worker进程是一个物理的Java虚拟机，执行拓扑的一部分任务。
如果拓扑的并发设置成了300，分配了50个Worker，那么每个Worker执行6个任务(作为Worker内部的线程）。
Config.TOPOLOGY_WORKERS: 这个配置设置了执行拓扑时分配Worker的数量。

Executor（执行者）
执行器只是Worker进程产生的单个线程。执行器运行一个或多个Task，这些Task都是相同的spout或bolt的实例。

Topology运行时的并发机制
当一个topology在storm cluster中运行时，它的并发主要跟3个逻辑实体相关：worker，executor 和task
1. Worker 是运行在工作节点上面，被Supervisor守护进程创建的用来执行计算的进程。每个Worker对应于一个给定topology的所有任务的一个子集。
   反过来说，一个Worker里面不会运行属于不同的topology的执行任务（不会出现1个worker为多个topology服务）。
2. Executor可以理解成一个Worker进程中的工作线程。一个Executor中只能运行隶属于同一个component（spout/bolt） 的task。
   一个Worker进程中可以有一个或多个Executor线程。默认是1个component只生成1个task（一个Executor运行一个task），executor线程里会在每次循环里顺序调用所有task实例。
3. 一个Executor可以负责1个或多个task。每个component（spout/bolt） 的并发度就是这个component对应的task数量。
   同时，task也是各个节点之间进行grouping（partition）的单位。

并发度的配置
有多种方法可以进行并发度的配置，其优先级如下：
storm.yaml < topology 私有配置 < component level（spout/bolt） 的私有配置
设置worker数量（通常配置一个cpu核心对应一个slot/worker）: Config#setNumWorkers()
设置executor数量: TopologyBuilder#setSpout()  /  TopologyBuilder#setBolt()
设置task数量: ComponentConfigurationDeclarer#setNumTasks()
topologyBuilder.setBolt("green-bolt", new GreenBolt(), 2)
               .setNumTasks(4)
               .shuffleGrouping(blue-spout);


任务(Task)
每个Spout和Bolt会以多个任务(Task)的形式在集群上运行。每个任务对应一个执行线程，
流分组定义了如何从一组任务(同一个Spout/Bolt)发送元组到另外一组任务(另外一个Bolt)上。
可以在调用TopologyBuilder的setSpout和setBolt函数时设置每个Spout和Bolt的并发数(Task数量)。

流分组(Stream groupings)
定义拓扑的时候，一部分工作是指定每个Bolt应该消费哪些流。流分组定义了一个流在一个消费它的Bolt内的多个任务(task)之间如何分组。
流分组跟计算机网络中的路由功能是类似的，决定了每个元组在拓扑中的处理路线。
在Storm中有七个内置的流分组策略，你也可以通过实现CustomStreamGrouping接口来自定义一个流分组策略:
1. 洗牌分组(Shuffle grouping): 随机分配元组到Bolt的某个任务上，这样保证同一个Bolt的每个任务都能够得到相同数量的元组。
2. 字段分组(Fields grouping): 按照指定的分组字段来进行流的分组。例如，流是用字段“user-id"来分组的，那有着相同“user-id"的元组就会分到同一个任务里。
   这是一种非常重要的分组方式，通过这种流分组方式，我们就可以做到让Storm产出的消息在这个"user-id"级别是严格有序的，
   这对一些对时序敏感的应用(例如，计费系统)是非常重要的。
4. All grouping: 流会复制给Bolt的所有任务。小心使用这种分组方式。在拓扑中，如果希望某类元祖发送到所有的下游消费者，就可以使用这种All grouping的流分组策略。
5. Global grouping: 整个流会分配给Bolt的一个任务。具体一点，会分配给有最小ID的任务。
6. 不分组(None grouping): 说明不关心流是如何分组的。目前，None grouping等价于洗牌分组。
8. Local or shuffle grouping：如果目标Bolt在同一个worker进程里有一个或多个任务，元组就会通过洗牌的方式分配到这些同一个进程内的任务里。
   否则，就跟普通的洗牌分组一样。这种方式的好处是可以提高拓扑的处理效率，因为worker内部通信就是进程内部通信了，相比拓扑间的进程间通信要高效的多。
   worker进程间通信是通过使用Netty来进行网络通信的。
3. Partial Key grouping: 跟字段分组一样，流也是用指定的分组字段进行分组的，但是在多个下游Bolt之间是有负载均衡的，这样当输入数据有倾斜时可以更好的利用资源。
7. Direct grouping：一种特殊的分组。对于这样分组的流，元组的生产者决定消费者的哪个任务会接收处理这个元组。
   只能在声明做直连的流(direct streams)上声明Direct groupings分组方式。只能通过使用emitDirect系列函数来吐元组给直连流。
   一个Bolt可以通过提供的TopologyContext来获得消费者的任务ID，也可以通过OutputCollector对象的emit函数(会返回元组被发送到的任务的ID)来跟踪消费者的任务ID。
InputDeclarer: 当调用TopologyBuilder的setBolt函数时会返回这个对象，它用来声明一个Bolt的输入流并指定流的分组方式。
CoordinatedBolt: 这个Bolt对于分布式的RPC拓扑很有用，大量使用了直连流(direct streams)和直连分组(direct groupings)。

可靠性(Reliability)
Storm保证了拓扑中Spout产生的每个元组都会被处理。Storm是通过跟踪每个Spout所产生的所有元组构成的树形结构并得知这棵树何时被完整地处理来达到可靠性。
每个拓扑对这些树形结构都有一个关联的“消息超时”。如果在这个超时时间里Storm检测到Spout产生的一个元组没有被成功处理完，
那Sput的这个元组就处理失败了，后续会重新处理一遍。
为了发挥Storm的可靠性，需要你在创建一个元组树中的一条边时告诉Storm，也需要在处理完每个元组之后告诉Storm。
这些都是通过Bolt吐元组数据用的OutputCollector对象来完成的。标记是在emit函数里完成，完成一个元组后需要使用ack函数来告诉Storm。


一个工作的Storm集群应该有一个Nimbus和一个或多个supervisors。另一个重要的节点是Apache ZooKeeper，它将用于nimbus和supervisors之间的协调。

Master结点(Master node)
在分布式系统中，调度服务非常重要，它的设计，会直接关系到系统的运行效率，错误恢复(fail over),故障检测(error detection)和水平扩展(scale)的能力。
集群上任务(task)的调度由一个Master节点来负责。这台机器上运行的Nimbus进程负责任务的调度。另外一个进程是Storm UI，可以web界面上查看集群和所有的拓扑的运行状态。
在配置文件storm.yaml中nimbus.seeds参数指定master节点：
nimbus.seeds: ["localhost"]

从节点(Slave node)
Storm集群上有多个从节点，他们从Nimbus上下载拓扑的代码，然后去真正执行。Slave上的Supervisor进程是用来监督和管理实际运行业务代码的进程。
在Storm 0.9之后，又多了一个进程Logviewer,可以用Storm UI来查看Slave节点上的log文件。
在配置文件storm.yaml中,决定了一台机器上运行几个worker:
supervisor.slots.ports:
- 6700
- 6701
- 6702

ZooKeeper的作用
ZooKeeper在Storm上不是用来做消息传输用的，而是用来提供协调服务(coordination service)，同时存储拓扑的状态和统计数据。
1. ZooKeeper相当于一块黑板，Supervisor，Nimbus和worker都在上面留下约定好的信息。
   例如Supervisor启动时，会在ZooKeeper上注册，Nimbus就可以发现Supervisor；Supervisor在ZooKeeper上留下心跳信息，
   Nimbus通过这些心跳信息来对Supervisor进行健康检测，检测出坏节点。
2. 由于Storm组件(component)的状态信息存储在ZooKeeper上，所以Storm组件就可以无状态，可以 kill -9来杀死
   例如：Supervisors/Nimbus的重启不影响正在运行中的拓扑，因为状态都在ZooKeeper上，从ZooKeeper上重新加载一下就好了
3. 用来做心跳
   Worker通过ZooKeeper把孩子executor的情况以心跳的形式汇报给Nimbus
   Supervisor进程通过ZK把自己的状态也以心跳的形式汇报给Nimbua
4. 存储最近任务的错误情况(拓扑停止时会删除)

Storm的容错(Fault Tolerance)机制
必须用工具如daemontools或者monit来监控Nimbus和Supervisor的后台进程。这样如果Nimbus或者Supervisor进程挂掉，会被daemontools检测到，并进行重启。
Nimbus和Supervisor进程被设计成快速失败(fail fast)的(当遇到异常的情况，进程就会挂掉)并且是无状态的(状态都保存在Zookeeper或者在磁盘上)。
最重要的是，worker进程不会因为Nimbus或者Supervisor挂掉而受影响。这跟Hadoop是不一样的，当JobTracker挂掉，所有的任务都会没了。
1. 当Nimbus(Master节点)挂掉会怎样？
   如果Nimbus是以推荐的方式处于进程监管(例如通过supervisord)之下，那它会被重启，不会有任何影响。
   否则当Nimbus挂掉后：
     已经存在的拓扑可以继续正常运行，但是不能提交新拓扑。
     正在运行的worker进程仍然可以继续工作。而且当worker挂掉，supervisor会一直重启worker。
     失败的任务不会被分配到其他机器(是Nimbus的职责)上了。
2. 当一个Supervisor(slave节点)挂掉会怎样？
   如果Supervisor是以推荐的方式处于进程监管(例如通过(supervisord)[supervisord.org/])之下，那它会被重启，不会有任何影响。
   否则当Supervisor挂掉: 分配到这台机器的所有任务(task)会超时，Nimbus会把这些任务(task)重新分配给其他机器。
3. 当一个worker挂掉会怎么样？
   当一个worker挂掉，supervisor会重启它。如果启动一直失败那么此时worker也就不能和Nimbus保持心跳了，Nimbus会重新分配worker到其他机器。
4. Nimbus算是一个单点故障吗？
   如果Nimbus节点挂掉，worker进程仍然可以继续工作。而且当worker挂掉，supervisor会一直重启worker。
   但是，没有了Nimbus，当需要的时候(如果worker机器挂掉了)worker就不能被重新分配到其他机器了。
   所以答案是，Nimbus在“某种程度”上属于单点故障的。在实际中，这种情况没什么大不了的，因为当Nimbus进程挂掉，不会有灾难性的事情发生

   
Storm如何保证可靠的消息处理：
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("sentences", new KestrelSpout("kestrel.backtype.com", 22133, "sentence_queue", new StringScheme()));
builder.setBolt("split", new SplitStentence(), 10).shuffleGrouping("sentences");
builder.setBolt("count", new WordCount(), 20).fieldsGrouping("split", new Fields("word"));

消息树的根节点是Spout产生的一个元组（通常是一行字符串）

一条消息被“完整处理”：
指一个从Spout发出的元组所触发的消息树中所有的消息都被Storm处理了。如果在指定的超时时间里，这个Spout元组触发的消息树中有任何一个消息没有处理完，
就认为这个Spout元组处理失败了。这个超时时间是通过每个拓扑的Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS配置项来进行配置的，默认是30秒。

消息被完整处理或者处理失败：
Spout发出的一个元组的生命周期。
public interface ISpout extends Serializable {
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector);
    void close();
    void nextTuple();
    void ack(Object msgId);
    void fail(Object msgId);
}
首先，Storm通过调用Spout的nextTuple函数来从Spout请求一个元组。Spout任务使用open函数入参中提供的SpoutOutputCollector来给Spout任务的某个输出流发射一个新元组。
当发射一个元组时，Spout提供了一个"消息标识"(message-id)，用来后续识别这个元组。
SpoutOutputCollector.emit(new Values("the cow jumped over the moon"), msgId);
接下来，元组就被发送到下游的Bolt进行消费，Storm会负责跟踪这个Spout元组创建的消息树。如果Storm检测到一个元组被完整地处理了，
Storm会调用产生这个元组的Spout任务的ack函数(由bolt的OutputCollector.ack(tuple)触发)，参数是Spout之前发送这个消息时提供给Storm的message-id。
当元组处理超时或处理失败时，Storm会在元组对应的Spout任务上调用fail函数，参数是之前Spout发送这个消息时提供给Storm的message-id。
这样应用程序通过实现Spout Bolt中的ack接口和fail接口来处理消息处理成功和失败的情况。


作为Storm用户，如果想利用Storm的可靠性，需要做两件事：
1. 创建一个新的元组时(消息树上创建一个新节点)需要通知Storm
   OutputCollector.emit(tuple, new Values());
   bolt是通过把输入的元组作为emit函数中的第一个参数来做锚定的。通过锚定，Storm就能够得到元组之间的关联关系(输入元组触发了新的元组)，
   继而构建出Spout元组触发的整个消息树。所以当下游处理失败时，就可以通知消息树根节点的Spout元组处理失败，让Spout重新处理。
   OutputCollector.emit(new Values());
   相反，如果在emit的时候没有指定输入的元组，叫做不锚定（新的元组没有加入消息树），这样下游处理失败，不能通知到上游的Spout任务。
2. 处理完一个元组，需要通知Storm
   OutputCollector.ack(tuple);
   锚定的作用就是指定元组树的结构，下一步是当元组树中某个元组已经处理完成时，通知Storm。通知是通过OutputCollector中的ack和fail函数来完成的。
   可以利用OutputCollector的fail函数来立即通知Storm，当前消息树的根节点Spout元组处理失败了。
   Storm需要占用内存来跟踪每个元组，所以每个被处理的元组都必须被确认。因为如果不对每个元组进行确认，任务最终会耗光可用的内存。

要实现ack机制：
1. spout发射tuple的时候指定messageId
2. spout要重写BaseRichSpout的fail和ack方法
3. spout对发射的tuple进行缓存，否则spout的fail方法收到acker发来的messsageId，spout也无法获取到发送失败的数据进行重发
4. spout根据messageId对于ack的tuple则从缓存队列中删除，对于fail的tuple可以选择重发。
5. spout重发时，为避免某个节点处理一直失败无限重发，最好能缓存messageid对应的重发次数。

Storm怎么处理重复的tuple？
   因为Storm要保证tuple的可靠处理，当tuple处理失败或者超时的时候，spout会fail并重新发送该tuple，那么就会有tuple重复计算的问题。
   这个问题是很难解决的，storm也没有提供机制帮助你解决。一些可行的策略：
1. 不处理，这也算是种策略。因为实时计算通常并不要求很高的精确度，后续的批处理计算会更正实时计算的误差。
2. 使用第三方集中存储来过滤，比如利用mysql、redis根据逻辑主键来去重。

   
Storm怎样高效的实现可靠性？
acker任务
一个Storm拓扑有一组特殊的"acker"任务，它们负责跟踪由每个Spout元组触发的消息的处理状态。当一个"acker"看到一个Spout元组产生的有向无环图中的消息被完全处理，
就通知当初创建这个Spout元组的Spout任务，这个元组被成功处理。可以通过拓扑配置项Config.TOPOLOGY_ACKER_EXECUTORS来设置一个拓扑中acker任务executor的数量。
Storm默认TOPOLOGY_ACKER_EXECUTORS和拓扑中配置的Worker的数量相同，对于需要处理大量消息的拓扑来说，需要增大acker executor的数量。
当拓扑的Spout或者Bolt中创建一个元组时，都会被赋予一个随机的64比特的标识(message-id)。acker任务使用这些id来跟踪每个Spout元组产生的有向无环图的处理状态。
在Bolt中产生一个新的元组时，会从锚定的一个或多个输入元组中拷贝所有Spout元组的message-id，所以每个元组都携带了自己所在元组树的根节点Spout元组的message-id。
当确认一个元组处理成功了，Storm就会给对应的acker任务发送特定的消息--通知acker当前这个Spout元组产生的消息树中某个消息处理完了。
Storm采用对元组中携带的Spout元组message-id哈希取模的方法来把一个元组映射到一个acker任务上(所以同一个消息树里的所有消息都会映射到同一个acker任务)。
当acker看到一个消息树被完全处理完，它就能根据处理的元组中携带的Spout元组message-id来确定产生这个Spout元组的taskid，
然后通知这个Spout任务消息树处理完成(调用 Spout任务的ack函数)。


去掉可靠性
如果可靠性无关紧要，例如你不关心元组失败场景下的消息丢失，那么你可以通过不跟踪元组的处理过程来提高性能。
不跟踪一个元组树会让传递的消息数量减半，因为正常情况下，元组树中的每个元组都会有一个确认消息。
另外，这也能减少每个元组需要存储的id的数量(指每个元组存储的Spout message-id)，减少了带宽的使用。
有三种方法来去掉可靠性：
1. 设置Config.TOPOLOGY_ACKERS为0。这种情况下，Storm会在Spout吐出一个元组后立马调用Spout的ack函数。这个元组树不会被跟踪。
2. 当产生一个新元组调用emit函数的时候通过忽略消息message-id参数来关闭这个元组的跟踪机制。
   collector.emit(new Values("the cow jumped over the moon"));
3. 如果你不关心某一类特定的元组处理失败的情况，可以在调用emit的时候不要使用锚定anchor。由于它们没有被锚定到某个Spout元组上，
   所以当它们没有被成功处理，不会导致Spout元组处理失败。
   collector.emit(new Values(word));

Storm如何避免数据丢失：
1. Bolt任务挂掉：导致一个元组没有被确认，这种场景下，这个元组所在的消息树中的根节点Spout元组会超时并被重新处理
2. acker任务挂掉：这种场景下，这个acker挂掉时正在跟踪的所有的Spout元组都会超时并被重新处理
3. Spout任务挂掉：这种场景下，需要应用自己实现检查点机制，记录当前Spout成功处理的进度，当Spout任务挂掉之后重启时，
   继续从当前检查点处理，这样就能重新处理失败的那些元组了。
   

Trident
Trident 是在 storm-core 之上的一个高级抽象，提供了对实时流的聚集，投影，过滤等操作，从而大大减少了开发Storm程序的工作量。
其可以保证 message 保证被处理且只被处理一次的语义，即 "exactly once"。
Trident 也包含 spout，其作用和在 storm-core 中相同，是整个 topology 的数据源。
Trident 中没有bolt，但有一个 operations 的概念，其作用和 bolt 相似，主要是实现一些对 message 的处理。
topology 中每个 spout 都会拥有一个唯一标识，且在整个集群中都唯一，这个标识是 spout 在 zookeeper 中记录的元数据的唯一标识。

Pipline
在 Trident 中，Spout emit message 不再是一条一条的，而是以一个 batch 的形式一次 emit 一组 messages。
默认的，storm 在同一只时间只会处理一个 batch，直到其成功或失败，通过topology.max.spout.pending可以配置其并发处理 batch 的个数。
但是 Trident 仍然会按顺序更新 batch 的 state 以保证『exactly once』语义。

Spout 类型
根据事务性可分为三类：
non-transactional spout （非事务性）、transactional spout （透明事务性）、opaque transactional spout （不透明事务性）
对应的 java 接口为：
IBatchSpout、IPartitionedTridentSpout、IOpaquePartitionedTridentSpout
另外，还有一个通用的非事务性接口 IRichSpout。

Operations
Trident 包含5中常用的 operation:
Partition-local operations
这个 operation 包含的操作都是本地的，即不会发生网络传输，这类操作都是独立的对每个 batch 生效的。
这一类是很通用的操作，其包含很多种类，常用的为以下 5 类：
1. Functions
Functions 是最通用的一类操作，这类操作对于每个待处理的 tuple，可以 emit 任意个结果，但是其不能删除或者变更 tuple 中已有的 fields，只能新增 fields。
2. Filters
Filter 与 Functions 不同，它是用来做过滤的，即处理的每个 tuple 只有两个选择：允许这个 tuple 继续向下传输或者不传输任何结果。
3. Map and FlatMap
Map 会处理接收到的 tuple，并 emit 一个新的 value，其是 1-1 的处理方式，即接收一个且 emit 一个。
FlatMap 和 map 类似，唯一的区别在于它会提交一组 values，即是 1-N 的处理方式，会 emit 一个 List<Values>。
4. min and minBy 和 max and maxBy
trident 是以一个小 batch 为单位处理处理 stream 中的数据的，这 4个类型的操作就是针对每次处理的这个 batch 计算最小/最大值。
5. Windowing
Trident 也提供了时间窗口的处理方式，和 storm-core 非常类似，通过 windowing 可以对同一时间窗口内的 batchs 进行计算、处理。
6. partitionAggregate
这类运算同样是针对每个 batch 而言的，它可以重新组合每个 batch 中的 tuples， 并 emit 任意结果。Trident提供了3类partitionAggregate：
CombinerAggregator：只会 emit 一个 tuple，且这个 tuple 只有一个 field
ReducerAggregator：也只会 emit 一个 tuple，这个 tuple 只有一个 value
Aggregator:  可以 emit 包含任意 fields 的任意数量 tuples，是一个比较通用的接口

Repartitioning operations
和Partition-local operations 相反，这类操作一定会发生网络上的传输。
1.shuffle
类似 storm-core 中的 shuffle grouping， 基于 Random Round Robin 算法随机将 tuples 均匀的传给目标 partition。
2. broadcast
类似 storm-core 的 all grouping，每个tuple 都会复制发送到后续所有的 partition。
3.partitionBy
类似 storm-core 的 Fields grouping，保证相同 fields 值数据被分配到统一个 partiton。
4.global
类似 storm-core 的 Global grouping，所有 tuples 被分配到同一个 partion。
5.batchGlobal
和 global 类似，但其会保证同一个 batch 的 tuples 被分配到同一个 partition。

Aggregation operations
这类操作是作用于 streams 之上的
注意与上文的 partitionAggregate 区别，这类操作是作用于 streams 之上的，而partitionAggregate 仅仅是对单个 batch的，即一个 batch 所拥有的本地操作。
这类操作可以分成两种：
1.aggregate：以 batch 为单位，每个 batch 独立实现相应的聚合计算。
2.persistentAggregate：与  aggregate  相反，
persistentAggregate 则是基于所有 batch  的所有 tuples 在全局实现聚合。
常用的聚合操作包括：ReducerAggregator、CombinerAggregator 以及通用的 Aggregator。其中 ReducerAggregator 和 Aggregator 会操作会将 stream repartition 到一个单独的 partition，在这个 partition 上实现聚合操作。而CombinerAggregator 则会现在每个 partition 上做实现 partial aggregation，然后将每个 partition 的结果在 repartition 到一个单独的 partition 实现聚合操作。
所以相比而言 CombinerAggregator 性能会更好。

Operations on grouped streams
这个操作只有一种，即 “groupby” ，功能类似 sql 中的 groupby，基于指定的 fields 分组，
此后的操作，比如 “persistentAggregate” 则不在以 batch 为单位，而是以不同的 group。

Merges and joins
这一类操作主要用于不同 stream 之间的计算，包含两种操作 “merge” 和 “join”。

DRPC
storm的DRPC模式的作用是实现从远程调用storm集群的计算资源，而不需要连接到集群的某一个节点。
storm实现DRPC主要是使用LinearDRPCTopologyBuilder这个类。

 * 
 * @author chenzq  
 * @date 2019年5月12日 下午1:02:45
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
package com.asiainfo.storm;