package cn.ce.binlog.mysql.parse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.pack.ClientAuthReqPacket;
import cn.ce.binlog.mysql.pack.HandshakeResPacket;
import cn.ce.binlog.mysql.pack.HeaderPacket;
import cn.ce.binlog.mysql.pack.ResultSetPacket;
import cn.ce.binlog.mysql.query.MysqlQueryExecutor;
import cn.ce.binlog.mysql.query.MysqlUpdateExecutor;
import cn.ce.binlog.mysql.util.ReadWriteUtil;

public class MysqlConnector implements Cloneable {
	private final static Log logger = LogFactory.getLog(MysqlConnector.class);

	private InetSocketAddress address;
	private String username;
	private String password;
	private AtomicBoolean isConnected = new AtomicBoolean(false);
	private SocketChannel channel;
	private String serverhost;
	private int serverPort;
	private MysqlConnector oldNc;
	private volatile AtomicBoolean prepareStop = new AtomicBoolean(false);

	public MysqlConnector(String serverhost, int serverPort, String username,
			String password) {
		this.serverhost = serverhost;
		this.serverPort = serverPort;
		this.address = new InetSocketAddress(this.serverhost, this.serverPort);
		this.username = username;
		this.password = password;

	}

	public MysqlConnector clone() {
		if (oldNc != null) {
			oldNc.disconnect();
		}
		MysqlConnector nc = new MysqlConnector(this.getServerhost(),
				this.getServerPort(), this.getUsername(), this.getPassword());
		oldNc = nc;
		return nc;
	}

	public ResultSetPacket query(String cmd) throws Exception {
		MysqlQueryExecutor exector = new MysqlQueryExecutor(this);
		return exector.query(cmd);
	}

	public void update(String cmd) throws Exception {
		MysqlUpdateExecutor exector = new MysqlUpdateExecutor(this);
		exector.update(cmd);
	}

	public boolean isConnected() {
		return this.channel != null && this.channel.isConnected();
	}

	public void connect() throws IOException {
		this.connect(30 * 1000);
	}

	public void connect(int soTimeout) throws IOException {
		if (isConnected.compareAndSet(false, true)) {
			channel = SocketChannel.open();
			try {
				setChannel(channel, soTimeout);
				// logger.info("connect MysqlConnection to " + address);
				channel.connect(address);
				HandshakeResPacket handshakeRes = handshake(channel);
				sendAuth(channel, handshakeRes);
				authRes(channel);
			} catch (Exception e) {
				// disconnect();
				throw new IOException("connect " + this.address + " failure:"
						+ ExceptionUtils.getStackTrace(e));
			}
		} else {
			throw new IOException("can't be connected twice.");
		}
	}

	public SocketChannel getChannel() {
		if (isConnected.get()) {
			return channel;
		}
		throw new RuntimeException("not connected,so no channel");
	}

	public void disconnect() {
		if (isConnected.compareAndSet(true, false)) {
			try {
				if (channel != null) {
					channel.close();
				}
				// logger.info("disConnect Mysql to " + address);
			} catch (Exception e) {
				throw new RuntimeException("disconnect " + this.address
						+ " failure", e);
			}
		} else {
			logger.warn("the channel " + this.address + " is not connected");
		}
		if (oldNc != null) {
			oldNc.disconnect();
		}

	}

	public void reconnect() throws IOException {
		// System.out.println("-----------重新链接---------------");
		disconnect();
		connect();
	}

	//
	private HandshakeResPacket handshake(SocketChannel channel)
			throws Exception {
		HeaderPacket header = new HeaderPacket();
		header.fromBytes(ReadWriteUtil.readBytes(channel, 4));
		byte[] handshakeResBody = ReadWriteUtil.readBytes(channel,
				header.getPacketBodyLength());
		HandshakeResPacket handshakeRes = new HandshakeResPacket(header);
		handshakeRes.fromBytes(handshakeResBody);
		return handshakeRes;
	}

	private void sendAuth(SocketChannel channel, HandshakeResPacket handshakeRes)
			throws IOException {
		byte[] dest = new byte[handshakeRes.getSeed().length
				+ handshakeRes.getRestOfScrambleBuff().length];
		System.arraycopy(handshakeRes.getSeed(), 0, dest, 0,
				handshakeRes.getSeed().length);
		System.arraycopy(handshakeRes.getRestOfScrambleBuff(), 0, dest,
				handshakeRes.getSeed().length,
				handshakeRes.getRestOfScrambleBuff().length);
		byte charsetNumber = 33;
		int seqnum = handshakeRes.getHeader().getPacketSequenceNumber() + 1;
		ClientAuthReqPacket clientAuth = new ClientAuthReqPacket(username,
				password, charsetNumber, "test",
				handshakeRes.getServerCapabilities(), dest, seqnum);
		ReadWriteUtil.write(
				channel,
				new ByteBuffer[] {
						ByteBuffer.wrap(clientAuth.getHeader().toBytes()),
						ByteBuffer.wrap(clientAuth.toBytes()) });
	}

	private void authRes(SocketChannel channel) throws Exception {
		HeaderPacket header = new HeaderPacket();
		header.fromBytes(ReadWriteUtil.readBytesAsBuffer(channel, 4).array());
		byte[] authResBody = ReadWriteUtil.readBytes(channel,
				header.getPacketBodyLength());
		assert authResBody != null;
		if (authResBody[0] < 0) {
			throw new IOException("auth failed");
		}
	}

	private void setChannel(SocketChannel channel, int soTimeout)
			throws IOException {
		channel.socket().setKeepAlive(true);
		channel.socket().setReuseAddress(true);
		channel.socket().setSoTimeout(soTimeout);
		channel.socket().setTcpNoDelay(true);
		channel.socket().setReceiveBufferSize(16 * 1024);
		channel.socket().setSendBufferSize(16 * 1024);
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getServerhost() {
		return serverhost;
	}

	public void setServerhost(String serverhost) {
		this.serverhost = serverhost;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public boolean isPrepareStop() {
		return prepareStop.get();
	}

	public void setPrepareStop(boolean prepareStop) {
		this.prepareStop.set(prepareStop);
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}

	public static void main(String[] args) {
		MysqlConnector c = null;
		try {
			c = new MysqlConnector("localhost", 3306, "root", "qwertyuiop");
			c.connect();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (c != null)
				c.disconnect();
		}
		System.out.println("--------OVER---------");
	}
}
