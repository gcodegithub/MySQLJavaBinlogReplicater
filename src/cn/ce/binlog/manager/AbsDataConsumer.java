package cn.ce.binlog.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.conv.ConsumeDaoIF;
import cn.ce.binlog.mysql.util.ThreadPoolUtils;
import cn.ce.binlog.session.BuzzWorker;
import cn.ce.binlog.vo.TransCommandVO;
import cn.ce.cons.Const;

public abstract class AbsDataConsumer {

	private final static Log logger = LogFactory.getLog(AbsDataConsumer.class);
	protected final static int maxRunThreadNum = Const.poolSize;
	protected ConsumeDaoIF curd;

	public ConsumeDaoIF getCurd() {
		return curd;
	}

	public void setCurd(ConsumeDaoIF curd) {
		this.curd = curd;
	}

	protected Map<String, List<Object>> shuffleMap(
			Map<String, List<Object>> rowVoListMap, int shffleNum) {
		Map<String, List<Object>> shuffledMap = new HashMap<String, List<Object>>();
		Integer count = 0;
		for (String oriKey : rowVoListMap.keySet()) {
			List<Object> oriList = rowVoListMap.get(oriKey);
			String shuffledKey = (new Integer(count % shffleNum)).toString();
			if (shuffledMap.containsKey(shuffledKey)) {
				List<Object> list = shuffledMap.get(shuffledKey);
				list.addAll(oriList);
			} else {
				List<Object> list = new ArrayList<Object>(oriList.size());
				list.addAll(oriList);
				shuffledMap.put(shuffledKey, oriList);
			}
			count++;
		}
		return shuffledMap;
	}

	protected void consumeFrame(final Context context,
			final ProcessInfo processInfo, Object[] params) throws Throwable {
		String slaveId = context.getSlaveId().toString();
		
			context.setConsuInSleep(false);
			context.setConsumerThread(Thread.currentThread());
			int len = this.getQueueSize(context);
			try {
				if (len > 0) {
					Object resVo = this.event2vo(context);
					TransCommandVO tcvo = this
							.getTransCommandVO(resVo, context);
					Map<String, List<Object>> rowVoListMap = tcvo
							.getRowVoListMap();
					if (len > 100 * maxRunThreadNum) {
						// 大数据量
						Map<String, List<Object>> shuffledMap = this
								.shuffleMap(rowVoListMap, maxRunThreadNum);
						int mapsize = shuffledMap.size();
						CountDownLatch latch = new CountDownLatch(mapsize);
						for (String key : shuffledMap.keySet()) {
							List<Object> list = shuffledMap.get(key);
							BuzzWorker<ConsumeDaoIF, List<Object>, CountDownLatch> worker = new BuzzWorker<ConsumeDaoIF, List<Object>, CountDownLatch>(
									curd, list, latch, "syncMySQL2DB");
							ThreadPoolUtils.doConsumeBuzzToExePool(worker);
						}
						while (true) {
							try {
								latch.await();
								break;
							} catch (InterruptedException ie) {
							}
						}
					} else {
						// 小数据量
						Map<String, List<Object>> shuffledMap = this
								.shuffleMap(rowVoListMap, 1);
						List<Object> list = new ArrayList<Object>();
						for (String key : shuffledMap.keySet()) {
							list.addAll(shuffledMap.get(key));
						}
						curd.syncMySQL2DB((ArrayList) list, null, null);
					}
					this.saveCheckPoint(resVo, context);
				} else {
					context.setConsuInSleep(true);
					Thread.sleep(5);
					context.setConsuInSleep(false);
				}
			} catch (InterruptedException ex) {
				context.setConsuInSleep(false);
				Thread.interrupted();
			}
		 

	}

	protected abstract int getQueueSize(Context context) throws Exception;

	protected abstract TransCommandVO getTransCommandVO(Object resVo,
			Context context) throws Exception;

	protected abstract Object event2vo(Context context) throws Exception;

	protected abstract void saveCheckPoint(Object resVo, Context context)
			throws Exception;
}
