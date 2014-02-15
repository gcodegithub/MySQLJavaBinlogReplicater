package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class IncidentLogEvent extends BinlogEvent {
	public static final int INCIDENT_NONE = 0;

	/** There are possibly lost events in the replication stream */
	public static final int INCIDENT_LOST_EVENTS = 1;

	/** Shall be last event of the enumeration */
	public static final int INCIDENT_COUNT = 2;

	private final int incident;
	private final String message;

	public IncidentLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		final int postHeaderLen = descriptionEvent.postHeaderLen[header
				.getType() - 1];

		buffer.position(commonHeaderLen);
		final int incidentNumber = buffer.getUint16();
		if (incidentNumber >= INCIDENT_COUNT || incidentNumber <= INCIDENT_NONE) {
			// If the incident is not recognized, this binlog event is
			// invalid. If we set incident_number to INCIDENT_NONE, the
			// invalidity will be detected by is_valid().
			incident = INCIDENT_NONE;
			message = null;
			return;
		}
		incident = incidentNumber;

		buffer.position(commonHeaderLen + postHeaderLen);
		message = buffer.getString();
	}

	public final int getIncident() {
		return incident;
	}

	public final String getMessage() {
		return message;
	}
}
