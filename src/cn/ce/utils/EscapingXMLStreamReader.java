package cn.ce.utils;

import java.util.HashSet;
import java.util.Set;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

public class EscapingXMLStreamReader implements XMLStreamReader {

	private final XMLStreamReader reader;
	public static final char substitute = '\uFFFD';
	private static final Set illegalChars = new HashSet();

	static {
		final String escapeString = "\u0000\u0001\u0002\u0003\u0004\u0005"
				+ "\u0006\u0007\u0008\u000B\u000C\u000E\u000F\u0010\u0011\u0012"
				+ "\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C"
				+ "\u001D\u001E\u001F\uFFFE\uFFFF";

		for (int i = 0; i < escapeString.length(); i++) {
			illegalChars.add(escapeString.charAt(i));
		}
	}

	public EscapingXMLStreamReader(XMLStreamReader reader) {

		if (null == reader) {
			throw new IllegalArgumentException("reader is null");
		} else {
			this.reader = reader;
		}
	}

	private boolean isIllegal(char c) {
		return illegalChars.contains(c);
	}

	private String escapeCharacters(String string) {

		char[] copy = null;
		boolean copied = false;
		for (int i = 0; i < string.length(); i++) {
			if (isIllegal(string.charAt(i))) {
				if (!copied) {
					copy = string.toCharArray();
					copied = true;
				}
				copy[i] = substitute;
			}
		}
		return copied ? new String(copy) : string;
	}

	public Object getProperty(String name) throws IllegalArgumentException {
		name = this.escapeCharacters(name);
		return reader.getProperty(name);
	}

	public int next() throws XMLStreamException {

		return reader.next();
	}

	public void require(int type, String namespaceURI, String localName)
			throws XMLStreamException {
		reader.require(type, namespaceURI, localName);

	}

	public String getElementText() throws XMLStreamException {
		String res = reader.getElementText();
		return this.escapeCharacters(res);
	}

	public int nextTag() throws XMLStreamException {
		return reader.nextTag();
	}

	public boolean hasNext() throws XMLStreamException {

		return reader.hasNext();
	}

	public void close() throws XMLStreamException {
		reader.close();
	}

	public String getNamespaceURI(String prefix) {
		return reader.getNamespaceURI(prefix);
	}

	public boolean isStartElement() {
		return reader.isStartElement();
	}

	public boolean isEndElement() {
		return reader.isEndElement();
	}

	public boolean isCharacters() {
		return reader.isCharacters();
	}

	public boolean isWhiteSpace() {
		return reader.isWhiteSpace();
	}

	public String getAttributeValue(String namespaceURI, String localName) {
		String res = reader.getAttributeValue(namespaceURI, localName);
		return this.escapeCharacters(res);
	}

	public int getAttributeCount() {
		return reader.getAttributeCount();
	}

	public QName getAttributeName(int index) {
		return reader.getAttributeName(index);
	}

	public String getAttributeNamespace(int index) {
		String res = reader.getAttributeNamespace(index);
		return this.escapeCharacters(res);
	}

	public String getAttributeLocalName(int index) {
		String res = reader.getAttributeLocalName(index);
		return this.escapeCharacters(res);
	}

	public String getAttributePrefix(int index) {
		String res = reader.getAttributePrefix(index);
		return this.escapeCharacters(res);
	}

	public String getAttributeType(int index) {
		String res = reader.getAttributeType(index);
		return this.escapeCharacters(res);
	}

	public String getAttributeValue(int index) {
		String res = reader.getAttributeValue(index);
		return this.escapeCharacters(res);
	}

	public boolean isAttributeSpecified(int index) {
		return reader.isAttributeSpecified(index);
	}

	public int getNamespaceCount() {
		return reader.getNamespaceCount();
	}

	public String getNamespacePrefix(int index) {
		String res = reader.getNamespacePrefix(index);
		return this.escapeCharacters(res);
	}

	public String getNamespaceURI(int index) {
		String res = reader.getNamespaceURI(index);
		return this.escapeCharacters(res);
	}

	public NamespaceContext getNamespaceContext() {
		return reader.getNamespaceContext();
	}

	public int getEventType() {
		return reader.getEventType();
	}

	public String getText() {
		String res = reader.getText();
		return this.escapeCharacters(res);
	}

	public char[] getTextCharacters() {
		char[] chars = reader.getTextCharacters();
		for (int i = 0; i < chars.length; i++) {
			if (isIllegal(chars[i])) {
				chars[i] = this.substitute;
			}
		}
		return chars;
	}

	public int getTextCharacters(int sourceStart, char[] target,
			int targetStart, int length) throws XMLStreamException {
		for (int i = 0; i < target.length; i++) {
			if (isIllegal(target[i])) {
				target[i] = this.substitute;
			}
		}
		return reader.getTextCharacters(sourceStart, target, targetStart,
				length);
	}

	public int getTextStart() {
		return reader.getTextStart();
	}

	public int getTextLength() {
		return reader.getTextLength();
	}

	public String getEncoding() {
		return reader.getEncoding();
	}

	public boolean hasText() {
		return reader.hasText();
	}

	public Location getLocation() {
		return reader.getLocation();
	}

	public QName getName() {
		return reader.getName();
	}

	public String getLocalName() {
		String res = reader.getLocalName();
		return this.escapeCharacters(res);
	}

	public boolean hasName() {

		return reader.hasName();
	}

	public String getNamespaceURI() {
		String res = reader.getNamespaceURI();
		return this.escapeCharacters(res);
	}

	public String getPrefix() {
		String res = reader.getPrefix();
		return this.escapeCharacters(res);
	}

	public String getVersion() {
		return reader.getVersion();
	}

	public boolean isStandalone() {
		return reader.isStandalone();
	}

	public boolean standaloneSet() {
		return reader.standaloneSet();
	}

	public String getCharacterEncodingScheme() {
		String res = reader.getCharacterEncodingScheme();
		return this.escapeCharacters(res);
	}

	public String getPITarget() {
		String res = reader.getPITarget();
		return this.escapeCharacters(res);
	}

	public String getPIData() {
		String res = reader.getPIData();
		return this.escapeCharacters(res);
	}

}
