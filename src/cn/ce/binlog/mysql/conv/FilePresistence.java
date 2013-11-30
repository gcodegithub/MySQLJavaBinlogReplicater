package cn.ce.binlog.mysql.conv;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;

import cn.ce.cons.Const;
import cn.ce.utils.JaxbContextUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.web.rest.vo.BinParseResultVO;

public class FilePresistence {

	private static JAXBContext binParseResultVOJaxbCTX = null;
	static {
		try {
			binParseResultVOJaxbCTX = JAXBContext
					.newInstance(BinParseResultVO.class);
		} catch (JAXBException e) {
			throw new RuntimeException(e);
		}
	}

	public void FilePersis(BinParseResultVO resVo, String serverhost,
			Long slaveId) throws Exception {
		if (resVo.getEventVOList().size() == 0) {
			return;
		}
		String absDirPath = ProFileUtil
				.findMsgString(Const.sysconfigFileClasspath,
						"bootstrap.mysql.vo.filepool.dir");
		absDirPath = absDirPath + "/" + serverhost + "_" + slaveId;
		if (this.noWrite(absDirPath, resVo, serverhost, slaveId)) {
			return;
		}
		String binfilename_end = resVo.getBinlogfilenameNext();
		String pos_end = resVo.getBinlogPositionNext().toString();
		binfilename_end = this.getbinfileSeq(binfilename_end);

		String tmpFileNameFullPath = absDirPath + "/" + binfilename_end + "_"
				+ pos_end + ".tmp";
		File target = new File(tmpFileNameFullPath);
		FileUtils.deleteQuietly(target);
		FileUtils.touch(target);

		// JaxbContextUtil.marshall(resVo, tmpFileNameFullPath);
		// BinParseResultVOJaxbCTX
		JaxbContextUtil.marshall(binParseResultVOJaxbCTX, resVo,
				tmpFileNameFullPath, false);
		ProFileUtil.checkIsExist(tmpFileNameFullPath, true);
		String fileNameFullPath = absDirPath + "/" + binfilename_end + "_"
				+ pos_end + ".xml";
		FileUtils.moveFile(new File(tmpFileNameFullPath), new File(
				fileNameFullPath));
		ProFileUtil.checkIsExist(fileNameFullPath, true);
		// this.tooManyFilesWarn(absDirPath);
	}

	private boolean noWrite(String absDirPath, BinParseResultVO resVo,
			String serverhost, Long slaveId) throws Exception {
		boolean isNotWrite = false;

		//
		String binfilename_end = resVo.getBinlogfilenameNext();
		String pos_end = resVo.getBinlogPositionNext().toString();
		//
		String posFileAbspath = ProFileUtil.findMsgString(
				Const.sysconfigFileClasspath,
				"binlogparse.checkpoint.fullpath.file");
		String filenameKey = slaveId + ".filenameKey";
		String binlogPositionKey = slaveId + ".binlogPosition";
		String binfilename_check = ProFileUtil.getValueFromProAbsPath(
				posFileAbspath, filenameKey);
		String pos_check = ProFileUtil.getValueFromProAbsPath(posFileAbspath,
				binlogPositionKey);
		if (binfilename_end.equals(binfilename_check)
				&& pos_end.equals(pos_check)) {
			// System.out.println("------------------数据的包内容和之前重复，不用持久化到文件");
			isNotWrite = true;
		}
		return isNotWrite;
	}

	private String getbinfileSeq(String fileBinSeq) {
		String seq = fileBinSeq.substring("mysql-bin.".length());
		return seq;
	}

}
