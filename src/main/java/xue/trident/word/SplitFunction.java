package xue.trident.word;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * 分割字符串的 Function
 * @author Administrator
 *
 */
public class SplitFunction extends BaseFunction{

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//获取tuple输入内容
		String subjects = tuple.getStringByField("subjects");
		//逻辑处理，然后发射给下一个组件
		for (String sub : subjects.split(" ")) {
			collector.emit(new Values(sub));
		}
	}
}
