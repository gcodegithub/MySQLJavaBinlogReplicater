package cn.ce.binlog.mysql.query;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ce.binlog.mysql.pack.ErrorPacket;
import cn.ce.binlog.mysql.pack.OKPacket;
import cn.ce.binlog.mysql.pack.QueryCommandPacket;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.util.PacketManager;

public class MysqlUpdateExecutor {

	private static final Logger logger = LoggerFactory
			.getLogger(MysqlUpdateExecutor.class);

	private SocketChannel channel;

	public MysqlUpdateExecutor(MysqlConnector connector) {
		if (!connector.isConnected()) {
			throw new RuntimeException(
					"should execute connector.connect() first");
		}

		this.channel = connector.getChannel();
	}

	public MysqlUpdateExecutor(SocketChannel ch) {
		this.channel = ch;
	}

	public OKPacket update(String updateString) throws Exception {
		QueryCommandPacket cmd = new QueryCommandPacket();
		cmd.setQueryString(updateString);
		byte[] bodyBytes = cmd.toBytes();
		PacketManager.write(channel, bodyBytes);

		logger.debug("read update result...");
		byte[] body = PacketManager.readBytes(channel, PacketManager
				.readHeader(channel, 4).getPacketBodyLength());
		if (body[0] < 0) {
			ErrorPacket packet = new ErrorPacket();
			packet.fromBytes(body);
			throw new IOException(packet + "\n with command: " + updateString);
		}

		OKPacket packet = new OKPacket();
		packet.fromBytes(body);
		return packet;
	}
}
