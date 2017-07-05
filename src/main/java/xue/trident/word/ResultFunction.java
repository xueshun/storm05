package xue.trident.word;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class ResultFunction extends BaseFunction{

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//获取tuple输入内容
		String sub = tuple.getStringByField("sub");
		Long count  = tuple.getLongByField("count");
		System.out.println(sub + " : " + count);
	}

}
