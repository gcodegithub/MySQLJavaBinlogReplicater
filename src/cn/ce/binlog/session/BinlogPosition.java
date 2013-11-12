package cn.ce.binlog.session;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class BinlogPosition implements Cloneable, Comparable<BinlogPosition> {

	/* binlog file's name */
	protected String fileName;

	/* position in file */
	protected long position;

	/**
	 * Binlog position init.
	 * 
	 * @param fileName
	 *            file name for binlog files: mysql-bin.000001
	 */
	public BinlogPosition(String fileName) {
		this.fileName = fileName;
		this.position = 0L;
	}

	/**
	 * Binlog position init.
	 * 
	 * @param fileName
	 *            file name for binlog files: mysql-bin.000001
	 */
	public BinlogPosition(String fileName, final long position) {
		this.fileName = fileName;
		this.position = position;
	}

	/**
	 * Binlog position copy init.
	 */
	public BinlogPosition(BinlogPosition source) {
		this.fileName = source.fileName;
		this.position = source.position;
	}

	public final String getFileName() {
		return fileName;
	}

	public final Long getPosition() {
		return position;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	/* Clone binlog position without CloneNotSupportedException */
	public BinlogPosition clone() {
		try {
			return (BinlogPosition) super.clone();
		} catch (CloneNotSupportedException e) {
			// Never happend
			return null;
		}
	}

	/**
	 * Compares with the specified fileName and position.
	 */
	public final int compareTo(String fileName, final long position) {
		final int val = this.fileName.compareTo(fileName);

		if (val == 0) {
			return (int) (this.position - position);
		}
		return val;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(BinlogPosition o) {
		final int val = fileName.compareTo(o.fileName);

		if (val == 0) {
			return (int) (position - o.position);
		}
		return val;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (obj instanceof BinlogPosition) {
			BinlogPosition pos = ((BinlogPosition) obj);
			return fileName.equals(pos.fileName)
					&& (this.position == pos.position);
		}
		return false;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
