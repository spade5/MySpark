# HARP：异构感知动态分区的微批流处理系统

## 摘要

微批流处理系统作为现代流式计算架构的核心范式，结合了批处理和流处理的优点，通过将连续数据流离散化为微批实现近实时高吞吐数据处理。其技术优势体现为：基于批次缓冲机制降低流式传输开销，利用缓存局部性优化提升计算效率，并通过分布式数据并行架构扩展至大规模集群。分布式数据并行依赖于数据分区过程，缓冲的微批通过特定的规则被分配到若干数据块中。然而，现有系统普遍基于同构环境设计，依赖均匀数据分区维持节点间负载均衡，难以适配当前数据中心广泛存在的异构算力环境，由于计算节点间硬件性能差异，均等划分的数据块将引发处理时间偏移，最终导致系统吞吐率显著下降。

针对上述挑战，本研究提出异构感知的动态分区与资源调度联合优化框架。在数据分区层面，构建基于 XGBoost 回归的异构算力模型，通过量化分析节点硬件指标（如 CPU 算力、内存容量）、数据特征（如批次大小、键值分布）与算子逻辑间的耦合关系，预测各节点处理时延；基于此设计贪心动态分区算法，以处理时间均衡为目标，实现异构集群的数据分区。在资源调度层面，提出算子特征驱动的差异化资源分配策略，结合流水线并行调度机制，依据算子计算密集度与资源敏感度调整资源分配，最大化异构资源利用率。

实验评估基于真实流式数据集与异构集群环境。结果表明：在保证端到端延迟的前提下，本方案较最新的权威工作 Prompt 提升吞吐率 27.5\%。本研究为异构友好型流式计算系统提供了理论框架与工程实践参考。

## 示例程序

```scala
    sparkConf.set("spark.streaming.blockGeneratorStyle", "regression") // 使用 HARP
    sparkConf.set("spark.streaming.regression.modelPath",
      "/home/chenhao/workspace/best_xgboost_model.json") // 模型路径
    sparkConf.set("spark.streaming.granularityFactor", 80) //粒度因子
    val ssc = new StreamingContext(sparkConf, Seconds(duration))

    val lines = ssc.socketTextStream("node21", 9000, StorageLevel.MEMORY_AND_DISK_SER)

    val wordCounts = lines.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFiles("/home/chenhao/output/counts/" + System.currentTimeMillis())

    ssc.start()
    ssc.awaitTermination()
```