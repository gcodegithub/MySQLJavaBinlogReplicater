package cn.ce.utils.common;

public abstract class WriteToFile {
	
	public abstract String writeFileProc();
	
	public String getOutFilePath(){
		String savePath = this.writeFileProc();
		return savePath;
	}
}
