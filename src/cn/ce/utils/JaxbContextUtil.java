package cn.ce.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.SAXException;

public class JaxbContextUtil {

	public static void marshall(JAXBContext ctx, Object toSerial,
			String targetFilePath, boolean isFormat) throws IOException,
			JAXBException, XMLStreamException {
		File file = new File(targetFilePath);
		FileOutputStream fo = new FileOutputStream(file);
		JaxbContextUtil.marshall(ctx, toSerial, fo, null, isFormat);
	}

	private static void marshall(JAXBContext ctx, Object toSerial,
			OutputStream out, String xsdName, boolean isFormat)
			throws IOException, XMLStreamException {
		try {
			XMLOutputFactory output = XMLOutputFactory.newInstance();
			XMLStreamWriter writer = output.createXMLStreamWriter(out, "UTF-8");
			EscapingXMLStreamWriter filter = new EscapingXMLStreamWriter(writer);

			Marshaller m = ctx.createMarshaller();
			m.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, isFormat);
			if (!StringUtils.isBlank(xsdName)) {
				m.setProperty("jaxb.noNamespaceSchemaLocation", xsdName);
			}
			m.marshal(toSerial, filter);
		} catch (JAXBException je) {
			je.printStackTrace();
			throw new IOException(je.getMessage());
		} finally {
			out.close();
		}
	}

	private static void marshall(Object toSerial, OutputStream out,
			String xsdName, boolean isFormat) throws IOException,
			JAXBException, XMLStreamException {
		JAXBContext ctx = JAXBContext.newInstance(toSerial.getClass());
		JaxbContextUtil.marshall(ctx, toSerial, out, xsdName, isFormat);
	}

	public static String marshallToString(Object toSerial, String xsdName,
			String encoding, boolean isFormat) throws IOException,
			JAXBException, XMLStreamException {
		// ------------------------------------生成xml字符串
		File tempFile = new File("engin_marshallToString.tmp"
				+ System.currentTimeMillis() + UUID.randomUUID());
		FileOutputStream out = new FileOutputStream(tempFile);
		JaxbContextUtil.marshall(toSerial, out, xsdName, isFormat);
		String xmlInfo = FileUtils.readFileToString(tempFile, encoding);
		tempFile.delete();
		return xmlInfo;
	}

	public static <T> T unmarshal(JAXBContext ctx, InputStream inputStream,
			URL xsdURL) throws JAXBException, IOException, SAXException {
		try {
			Unmarshaller u = ctx.createUnmarshaller();
			if (xsdURL != null) {
				SchemaFactory schemaFactory = SchemaFactory
						.newInstance("http://www.w3.org/2001/XMLSchema");
				Schema schema = schemaFactory.newSchema(xsdURL);
				u.setSchema(schema);
			}
			T t = (T) u.unmarshal(inputStream);
			return t;
		} catch (JAXBException je) {
			je.printStackTrace();
			throw new IOException(je.getMessage());
		} finally {
			inputStream.close();
		}
	}

	public static <T> T unmarshal(Class<T> docClass, InputStream inputStream,
			URL xsdURL) throws JAXBException, IOException, SAXException,
			XMLStreamException {
		try {
			JAXBContext ctx = JAXBContext.newInstance(docClass);
			Unmarshaller u = ctx.createUnmarshaller();
			if (xsdURL != null) {
				SchemaFactory schemaFactory = SchemaFactory
						.newInstance("http://www.w3.org/2001/XMLSchema");
				Schema schema = schemaFactory.newSchema(xsdURL);
				u.setSchema(schema);
			}
			XMLInputFactory input = XMLInputFactory.newInstance();
			XMLStreamReader reader = input.createXMLStreamReader(inputStream,
					"UTF-8");
			EscapingXMLStreamReader filter = new EscapingXMLStreamReader(reader);
			T t = (T) u.unmarshal(filter);
			return t;
		} catch (JAXBException je) {
			je.printStackTrace();
			throw new IOException(je.getMessage());
		} finally {
			inputStream.close();
		}

	}

	public static <T> T unmarshallToObject(Class<T> docClass, String infoXml,
			URL xsdURL, String encoding) throws IOException, JAXBException,
			SAXException, XMLStreamException {
		ByteArrayInputStream in = new ByteArrayInputStream(
				infoXml.getBytes(encoding));
		T t = (T) JaxbContextUtil.unmarshal(docClass, in, xsdURL);
		return t;
	}
}
