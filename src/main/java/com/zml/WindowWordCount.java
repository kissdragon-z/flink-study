package com.zml;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        // 设置主机名、分隔符、端口号
        String hostname = "localhost";
        String delimiter = "\n";
        int port = 8888;
//        // 使用parameterTool，通过控制台获取参数
//        try {
//            ParameterTool= parameterTool = ParameterTool.fromArgs(args);
//            port = parameterTool.getInt("port") ;
//        }catch (Exception e){
//            // 如果没有传入参数，则赋默认值
//            System.out.println("No port set. use default port 9000--java");
//            port = 9999 ;
//        }

        //1、获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream(
                hostname, port, delimiter);

        // 输入：a a c
        // 输出：（即flatMap操作）
        // a 1
        // a 1
        // c 1

        //3、transformation操作，对数据流实现计算
        // FlatMapFunction<T, O>: T代表输入格式，O代表返回格式
        DataStream<WordWithCount> windowCounts = text
                // 3.1、将用户输入的文本流以非空白符的方式拆开来，得到单个的单词，
                // 存入命名为out的Collector中
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out)
                            throws Exception {
                        String[] splits = value.split("\\s");  // 通过空白符或制表符切开
                        for (String word : splits) {
                            out.collect(new WordWithCount(word, 1L));   // 为输出写数据
                        }

                    }
                })
                // 3.2、将输入的文本分为不相交的分区，每个分区包含的都是具有相同key的元素。
                // 也就是说，相同的单词被分在了同一个区域，下一步的reduce就是统计分区中的个数
                .keyBy("word")
                // 3.3、滑动窗口三个字，指定时间窗口大小为2秒，指定时间间隔为1秒
                .timeWindow(Time.seconds(2), Time.seconds(1))
                // 3.4、一个在KeyedDataStream上“滚动”进行的reduce方法。
                // 将上一个reduce过的值和当前element结合，产生新的值并发送出。
                // 此处是说，对输入的两个对象进行合并，统计该单词的数量和
                // 这里使用 sum 或 reduce 都可以
                //.sum("count") ;  // 是对 reduce 的封装实现
                // reduce 返回类型 SingleOutputStreamOperator，继承了 DataStream
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // 4、把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        // 这一行代码一定要实现，否则程序不执行
        // 报错：Unhandled exception: java.lang.Exception
        // 需要对 main 添加异常捕获
        System.out.println("start.....");
        env.execute("Socket window count");
    }

    // 自定义统计单词的数据结构，包含两个变量和三个方法
    public static class WordWithCount {
        //两个变量存储输入的单词及其数量
        public String word;
        public long count;

        // 空参的构造函数
        public WordWithCount() {
        }

        // 通过外部传参赋值的构造函数
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        // 打印显示 word，count
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
