package cn.ce.utils.common;

import javax.xml.parsers.*;
import org.xml.sax.*;
import javax.xml.transform.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.Transformer;
import org.w3c.dom.DocumentType;
import org.w3c.dom.DOMImplementation;
import java.io.*;
import org.w3c.dom.*;
import org.w3c.dom.ls.*;
import javax.xml.transform.stream.StreamSource;

public class DOMUtil {
	// 从XML文件中得到DOM对象
	public static Document generateDocByFile(File sourceFile)
			throws ParserConfigurationException, IOException, SAXException {
		DocumentBuilderFactory dbfactory = DocumentBuilderFactory.newInstance();
		dbfactory.setNamespaceAware(true);
		dbfactory.setValidating(false);
		// 忽略无用空白
		dbfactory.setIgnoringElementContentWhitespace(true);
		// 得到解析器
		DocumentBuilder domparser = dbfactory.newDocumentBuilder();
		// 对文档进行解析，得到文档对象
		Document doc = domparser.parse(sourceFile);
		return doc;
	}

	// 用DOM规范将生DOM对象树输出到输出流中
	public static void saveToOutputStreamUseDom(Document doc, String encoding,
			OutputStream outputStream) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			DOMImplementation impl = builder.getDOMImplementation();
			DOMImplementationLS implls = (DOMImplementationLS) impl;
			// 序列化工具
			LSSerializer domWriter = implls.createLSSerializer();
			// DOM树格式配置
			DOMConfiguration domConfig = domWriter.getDomConfig();
			boolean isSupport = domConfig.canSetParameter(
					"format-pretty-print", new Boolean("true"));
			if (isSupport) {
				domConfig.setParameter("format-pretty-print", new Boolean(
						"true"));
			} else {
				System.out.println("不支持美化文档！");
			}
			LSOutput output = implls.createLSOutput();
			output.setByteStream(outputStream);
			output.setEncoding(encoding);
			domWriter.write(doc, output);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 用JAXP将生DOM对象树输出到输出流中
	public static OutputStream saveToOutputStream(Document doc,
			String encoding, OutputStream out)
			throws TransformerConfigurationException, TransformerException {
		// 转换工厂
		TransformerFactory tf = TransformerFactory.newInstance();
		// 转换器
		Transformer transformer = tf.newTransformer();
		// 编码
		transformer.setOutputProperty(OutputKeys.ENCODING, encoding);
		// 含有缩进
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		// 得到要打印得xml文档
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(out);
		// 进行转换
		transformer.transform(source, result);
		return out;
	}

	// 建立一个空的DOM文档树对象
	public static Document generateNullDoc()
			throws ParserConfigurationException {
		// Builder的工厂
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		// 创建一个Builder实例
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();
		return doc;
	}

	// 生成一个DOCTYPE
	public static DocumentType generateDocType(String rootElement,
			String publicId, String dtdURI, String defaultNameSpace)
			throws Exception {
		// Builder的工厂
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		// 创建一个Builder实例
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation docImpl = builder.getDOMImplementation();
		DocumentType docType = docImpl.createDocumentType(rootElement,
				publicId, dtdURI);
		return docType;
	}

	// 建立一个带文档类型的XML文档
	// "-//My Software Foundation//DTD XML TEST 1.0//CN"
	public static Document generateDocWithDocType(String rootElement,
			String publicId, String dtdURI, String defaultNameSpace)
			throws Exception {
		// Builder的工厂
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		// 创建一个Builder实例
		DocumentBuilder builder = factory.newDocumentBuilder();
		DOMImplementation docImpl = builder.getDOMImplementation();
		DocumentType docType = docImpl.createDocumentType(rootElement,
				publicId, dtdURI);
		Document doc = docImpl.createDocument(defaultNameSpace, rootElement,
				docType);
		return doc;
	}

	public static void xsltConvert(String xmlSourceFile, String xsltSourceFile,
			OutputStream out) {
		try {
			File xmlFile = new File(xmlSourceFile);
			File xsltFile = new File(xsltSourceFile);

			Source xmlSource = new StreamSource(xmlFile);
			Source xsltSource = new StreamSource(xsltFile);
			Result result = new StreamResult(out);

			TransformerFactory transFact = TransformerFactory.newInstance();
			Transformer trans = transFact.newTransformer(xsltSource);
			// 给参数传递值
			// trans.setParameter("image","B.jpg");
			// trans.clearParameters();
			trans.transform(xmlSource, result);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void copy(InputStream in, OutputStream out)
			throws IOException {
		// do not allow other threads to read from the
		// input or write to the output while copying is
		// taking place
		synchronized (in) {
			synchronized (out) {

				byte[] buffer = new byte[256];
				while (true) {
					int bytesRead = in.read(buffer);
					if (bytesRead == -1)
						break;
					out.write(buffer, 0, bytesRead);
				}
			}
		}
	}

}
