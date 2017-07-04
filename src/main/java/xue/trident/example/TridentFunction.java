package xue.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

/**
 * TridentFunction使用
 * @author Administrator
 *
 */
public class TridentFunction {
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		//设置batch最大处理
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(20);
		if(args.length==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-function", conf, buildTopology());
			Thread.sleep(100000);
		}else{
			StormSubmitter.submitTopology("trident-function", conf, buildTopology());
		}
	}

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		//设定数据源
		@SuppressWarnings("unchecked")
		/**
		 * 参数1. 声明输入的域字段为 a,b,c,d
		 * 参数2. 设置批处理大小
		 * 参数3. 测试数据  设置数据源内容
		 */
		FixedBatchSpout spout = new FixedBatchSpout(
				new Fields("a","b","c","d"), 
				4, 
				new Values(1,4,7,10),
				new Values(1,1,3,11),
				new Values(2,2,7,1),
				new Values(2,5,7,2));
		
		//指定是否循环
		spout.setCycle(false);
		//指定输入源Spout
		Stream inputstream = topology.newStream("spout", spout);
		
		/**
		 * 要实现流 spout - bolt 的模式 在trident里使用each来实现
		 * 
		 * each 方法参数
		 * 	参数1. 输入数据源参数名称： a , b , c , d
		 *  参数2. 需要流转的执行function对象（也就是bolt对象） ： new Function()
		 *  参数3. 指定队形里的输出参数名称 sum
		 */
		inputstream.each(new Fields("a","b","c","d"), new SumFunction(),new Fields("sum"))
			/**
			 * 继续使用each调用下一个Function（bolt）
			 * 参数1. 输入数据源参数名称  a , b , c , d , sum
			 * 参数2. new Result(),也就是下个执行函数，、
			 * 参数3. 没有任何输出
			 */
			.each(new Fields("a","b","c","d","sum"), new Result(),new Fields());
		return topology.build();
	}
	
	/**
	 * 继承BaseFunction类，重载execute方法
	 * @author Administrator
	 *
	 */
	public static class SumFunction extends BaseFunction{
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			System.out.println("传入的数据：" + tuple);
			//获取a,b两个域
			int a = tuple.getInteger(0);
			int b = tuple.getInteger(1);
			int sum = a + b;
			//发射数据
			collector.emit(new Values(sum));
		}
		
	}
	
	public static class Result extends BaseFunction{
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			//获取tuple输入的数据
			System.out.println(tuple);
			Integer a = tuple.getIntegerByField("a");
			Integer b = tuple.getIntegerByField("b");
			Integer c = tuple.getIntegerByField("c");
			Integer d = tuple.getIntegerByField("d");
			Integer sum = tuple.getIntegerByField("sum");
			System.out.println("a:" + a +",b:" + b + ",c:" + c + ",d:" + d + ",sum:" +sum);
		}
	}
}
