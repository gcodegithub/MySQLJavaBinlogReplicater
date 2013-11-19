package cn.ce.binlog.mysql.pack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.util.MySQLPasswordEncrypter;
import cn.ce.binlog.mysql.util.ReadWriteUtil;

public class ClientAuthReqPacket implements IPacket {
	private String username;
	private String password;
	private byte charsetNumber;
	private String databaseName;
	private int serverCapabilities;
	private byte[] scrumbleBuff;
	private HeaderPacket header;
	private byte[] clientAuthPkgBody;

	public ClientAuthReqPacket(String username, String password,
			byte charsetNumber, String databaseName, int serverCapabilities,
			byte[] scrumbleBuff, int sequenceNumber) throws IOException {
		super();
		this.username = username;
		this.password = password;
		this.charsetNumber = charsetNumber;
		this.databaseName = databaseName;
		this.serverCapabilities = serverCapabilities;
		this.scrumbleBuff = scrumbleBuff;
		this.clientAuthPkgBody = this.toBytes();
		this.header = new HeaderPacket();
		this.header.setPacketBodyLength(clientAuthPkgBody.length);
		this.header.setPacketSequenceNumber((byte) sequenceNumber);

	}

	public void fromBytes(byte[] data) {
		throw new RuntimeException("not needed");
	}

	/**
	 * <pre>
	 * VERSION 4.1
	 *  Bytes                        Name
	 *  -----                        ----
	 *  4                            client_flags
	 *  4                            max_packet_size
	 *  1                            charset_number
	 *  23                           (filler) always 0x00...
	 *  n (Null-Terminated String)   user
	 *  n (Length Coded Binary)      scramble_buff (1 + x bytes)
	 *  n (Null-Terminated String)   databasename (optional)
	 * </pre>
	 * 
	 * @throws IOException
	 */
	public byte[] toBytes() throws IOException {
		if (clientAuthPkgBody != null) {
			return clientAuthPkgBody;
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		// 1. write client_flags
		// 1|4|512|1024|8192|32768
		/**
		 * CLIENT_LONG_PASSWORD CLIENT_LONG_FLAG CLIENT_PROTOCOL_41
		 * CLIENT_INTERACTIVE CLIENT_TRANSACTIONS CLIENT_SECURE_CONNECTION
		 */
		ReadWriteUtil.writeUnsignedIntLittleEndian(1 | 4 | 512 | 8192 | 32768,
				out); // remove
						// client_interactive
						// feature

		// 2. write max_packet_size
		ReadWriteUtil.writeUnsignedIntLittleEndian(IPacket.MAX_PACKET_LENGTH,
				out);
		// 3. write charset_number
		out.write(this.charsetNumber);
		// 4. write (filler) always 0x00...
		out.write(new byte[23]);
		// 5. write (Null-Terminated String) user
		ReadWriteUtil.writeNullTerminatedString(getUsername(), out);
		// 6. write (Length Coded Binary) scramble_buff (1 + x bytes)
		if (StringUtils.isEmpty(getPassword())) {
			out.write(0x00);
		} else {
			try {
				byte[] encryptedPassword = MySQLPasswordEncrypter.scramble411(
						getPassword().getBytes(), scrumbleBuff);
				ReadWriteUtil.writeBinaryCodedLengthBytes(encryptedPassword,
						out);
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(
						"can't encrypt password that will be sent to MySQL server.",
						e);
			}
		}
		// 7 . (Null-Terminated String) databasename (optional)
		if (getDatabaseName() != null) {
			ReadWriteUtil.writeNullTerminatedString(getDatabaseName(), out);
		}
		// end write
		return out.toByteArray();
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

	public void setCharsetNumber(byte charsetNumber) {
		this.charsetNumber = charsetNumber;
	}

	public byte getCharsetNumber() {
		return charsetNumber;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setServerCapabilities(int serverCapabilities) {
		this.serverCapabilities = serverCapabilities;
	}

	public int getServerCapabilities() {
		return serverCapabilities;
	}

	public void setScrumbleBuff(byte[] scrumbleBuff) {
		this.scrumbleBuff = scrumbleBuff;
	}

	public byte[] getScrumbleBuff() {
		return scrumbleBuff;
	}

	public HeaderPacket getHeader() {
		return header;
	}

	public void setHeader(HeaderPacket header) {
		this.header = header;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
