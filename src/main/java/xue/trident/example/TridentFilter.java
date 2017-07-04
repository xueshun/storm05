package xue.trident.example;



import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

/**
 * Storm Trident使用
 * @author Administrator
 *
 */
public class TridentFilter {
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		//设置batch最大处理
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(20);
		if(args.length==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-function", conf, buildTopology());
			Thread.sleep(100000);
			cluster.shutdown();
		}else{
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
	
	public static StormTopology buildTopology(){
		TridentTopology topology = new TridentTopology();
		
		//设定数据源
		/**
		 * 参数1. 声明输入的域字段为'a','b','c','d'
		 * 参数2. 设置批处理大小可以为1，也可以为4， 为1的时候是一条数据发送，为4的时候是4条数据一起发送
		 * 参数3. 测试数据，设置数据源内容
		 */
		@SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(
				new Fields("a","b","c","d"), 
				4, 
				new Values(1,4,7,10),
				new Values(1,1,3,11),
				new Values(2, 2, 7, 1),
				new Values(2, 5, 7, 2));
		
		//指定是否循环
		spout.setCycle(false);
		//指定输入源spout
		Stream inputStream = topology.newStream("spout", spout);
		
		/**
		 * 要实现spout - bolt的模式 在trident里是使用each来实现
		 * 
		 * each的方法参数
		 * 	参数1.输入数据源名称：subjects
		 *  参数2.需要流转执行的Function对象（也就是bolt对象）：new Split()
		 */
		inputStream.each(new Fields("a", "b", "c", "d"), new CheckFilter())
			/*
			 * 继续使用each 调用下一个function(bolt)输入的参数为subject和count,
			 * 	第二个参数为 new Result(), 也就是执行函数，
			 * 	第三个参数为没有输出
			 */
          .each(new Fields("a", "b", "c", "d"), new Result(), new Fields());
		
		//利用这种方式，我们返回一个StormTopology对象进行提交。
		return topology.build();
	}
	
	/*
	 * 继承BaseFileter类，重载isKeep方法
	 */
	public static class CheckFilter extends BaseFilter{
		@Override
		public boolean isKeep(TridentTuple tuple) {
			int a = tuple.getInteger(0);
			int b = tuple.getInteger(2);
			if((a+b)%2 == 0){
				return true;
			}
			return false;
			
		}
		
	}
	
	/*
	 * 继承BaseFunction类，重载execute方法
	 */
	public static class Result extends BaseFunction{
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			//获取tuple输入内容
			Integer a = tuple.getIntegerByField("a");
			Integer b = tuple.getIntegerByField("b"); 
			Integer c = tuple.getIntegerByField("c");
			Integer d = tuple.getIntegerByField("d");
			System.out.println("a:" + a + ",b:" + b + ",c:" + c +",d:" +d);
		}
		
	}
}
