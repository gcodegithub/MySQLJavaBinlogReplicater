package cn.ce.binlog.mysql.query;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import cn.ce.binlog.mysql.pack.ErrorPacket;
import cn.ce.binlog.mysql.pack.FieldPacket;
import cn.ce.binlog.mysql.pack.HeaderPacket;
import cn.ce.binlog.mysql.pack.QueryCommandPacket;
import cn.ce.binlog.mysql.pack.ResultSetHeaderPacket;
import cn.ce.binlog.mysql.pack.ResultSetPacket;
import cn.ce.binlog.mysql.pack.RowDataPacket;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.util.PacketManager;

public class MysqlQueryExecutor {

	private SocketChannel channel;

	public MysqlQueryExecutor(MysqlConnector connector) {
		if (!connector.isConnected()) {
			throw new RuntimeException(
					"should execute connector.connect() first");
		}

		this.channel = connector.getChannel();
	}

	public MysqlQueryExecutor(SocketChannel ch) {
		this.channel = ch;
	}

	/**
	 * (Result Set Header Packet) the number of columns <br>
	 * (Field Packets) column descriptors <br>
	 * (EOF Packet) marker: end of Field Packets <br>
	 * (Row Data Packets) row contents <br>
	 * (EOF Packet) marker: end of Data Packets
	 * 
	 * @param queryString
	 * @return
	 * @throws Exception
	 */
	public ResultSetPacket query(String queryString) throws Exception {
		QueryCommandPacket cmd = new QueryCommandPacket();
		cmd.setQueryString(queryString);
		byte[] bodyBytes = cmd.toBytes();
		PacketManager.write(channel, bodyBytes);
		byte[] body = readNextPacket();

		if (body[0] < 0) {
			ErrorPacket packet = new ErrorPacket();
			packet.fromBytes(body);
			throw new IOException(packet + "\n with command: " + queryString);
		}

		ResultSetHeaderPacket rsHeader = new ResultSetHeaderPacket();
		rsHeader.fromBytes(body);

		List<FieldPacket> fields = new ArrayList<FieldPacket>();
		for (int i = 0; i < rsHeader.getColumnCount(); i++) {
			FieldPacket fp = new FieldPacket();
			fp.fromBytes(readNextPacket());
			fields.add(fp);
		}

		readEofPacket();

		List<RowDataPacket> rowData = new ArrayList<RowDataPacket>();
		while (true) {
			body = readNextPacket();
			if (body[0] == -2) {
				break;
			}
			RowDataPacket rowDataPacket = new RowDataPacket();
			rowDataPacket.fromBytes(body);
			rowData.add(rowDataPacket);
		}

		ResultSetPacket resultSet = new ResultSetPacket();
		resultSet.getFieldDescriptors().addAll(fields);
		for (RowDataPacket r : rowData) {
			resultSet.getFieldValues().addAll(r.getColumns());
		}
		resultSet.setSourceAddress(channel.socket().getRemoteSocketAddress());

		return resultSet;
	}

	private void readEofPacket() throws Exception {
		byte[] eofBody = readNextPacket();
		if (eofBody[0] != -2) {
			throw new IOException(
					"EOF Packet is expected, but packet with field_count="
							+ eofBody[0] + " is found.");
		}
	}

	protected byte[] readNextPacket() throws Exception {
		HeaderPacket h = PacketManager.readHeader(channel, 4);
		return PacketManager.readBytes(channel, h.getPacketBodyLength());
	}
}
