package cn.ce.utils.common;

import java.util.List;

public class ExportExcelEntity {


	/*
	 * 表头字体大小
	 */
	private transient int tableHeadFontSize = 12;
	/*
	 * 数据字体大小
	 */
	private transient int dataFontSize = 12;
	/*
	 * 是否成功导出数据
	 */
	private transient boolean success;
	/*
	 * 不成功返回的错误信息
	 */
	private transient String error;
	/*
	 * sheet的名称
	 */
	private transient String SheetName;
	/*
	 * Excel保存的路径(包含文件名)
	 */
	private transient String SavePath = "temp";
	/*
	 * 显示的中文字段名称
	 */
	private transient String[] ChiColsName;
	/*
	 * 显示的中文字段名称对应的英文字段名称
	 */
	private transient String[] EnColsName;
	/*
	 * 导出字段宽度
	 */
	private transient int[] ColsWidth;
	/*
	 * 导出数据字段对齐方式(0－left，1－right，2－center)
	 */
	private transient int[] ColsAlign;
	/*
	 * 导出标题对齐方式(0－left，1－right，2－center)
	 */
	private transient int[] ColsHeadAlign;
	/*
	 * 导出的数据集
	 */
	private transient List<?> ExportList;
	/*
	 * 文件存在是否覆盖
	 */
	private transient boolean CoverFile = true;
	/*
	 * 是否有标题
	 */
	private transient boolean HasTitle = true;
	/*
	 * 导出的数据集是否是从界面传入
	 */
	private transient boolean IsPageList = false;
	/*
	 * 导出数据类型 如果不设置，则默认为文本类型 （0－文本，1－数值型）
	 */
	private transient int[] DataType;
	/*
	 * 数值类型的小数位数 此设置在导出数据类型设置为1时有效 如果未设置，且设置的数据类型为1时，默认为2位小数
	 */
	private transient int[] DecimalLen;
	/*
	 * 导出数据列是否是货币形式 当导出的数据类型为文本时，此设置可用，否则设置无效。
	 * 如果未设置，如果对齐方式是右对齐，则默认为1的方式，如果是左对齐则默认是0的方式 0－否（1234），1－是（1,234）
	 */
	private transient int[] IsCurrency;
	/*
	 * 错误编号(0-成功,)
	 */
	private transient String ErrorNumber = "0";
	/*
	 * 错误编号(0-成功,)
	 */
	private transient boolean bMulHead = false;

	public boolean isHasTitle() {
		return HasTitle;
	}

	public void setHasTitle(boolean hasTitle) {
		HasTitle = hasTitle;
	}

	public boolean isCoverFile() {
		return CoverFile;
	}

	public void setCoverFile(boolean coverFile) {
		CoverFile = coverFile;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getSheetName() {
		return SheetName;
	}

	public void setSheetName(String sheetName) {
		SheetName = sheetName;
	}

	public String getSavePath() {
		return SavePath;
	}

	public void setSavePath(String savePath) {
		SavePath = savePath;
	}

	public String[] getChiColsName() {
		return ChiColsName;
	}

	public void setChiColsName(String[] chiColsName) {
		ChiColsName = chiColsName;
	}

	public String[] getEnColsName() {
		return EnColsName;
	}

	public void setEnColsName(String[] enColsName) {
		EnColsName = enColsName;
	}

	public List<?> getExportList() {
		return ExportList;
	}

	public void setExportList(List<?> exportList) {
		ExportList = exportList;
	}

	public int[] getColsWidth() {
		return ColsWidth;
	}

	public void setColsWidth(int[] colsWidth) {
		ColsWidth = colsWidth;
	}

	public int[] getColsAlign() {
		return ColsAlign;
	}

	public void setColsAlign(int[] colsAlign) {
		ColsAlign = colsAlign;
	}

	public boolean isPageList() {
		return IsPageList;
	}

	public void setPageList(boolean isPageList) {
		IsPageList = isPageList;
	}

	public String getErrorNumber() {
		return ErrorNumber;
	}

	public void setErrorNumber(String errorNumber) {
		ErrorNumber = errorNumber;
	}

	public int[] getDataType() {
		return DataType;
	}

	public void setDataType(int[] dataType) {
		DataType = dataType;
	}

	public int[] getIsCurrency() {
		return IsCurrency;
	}

	public void setIsCurrency(int[] isCurrency) {
		IsCurrency = isCurrency;
	}

	public int[] getDecimalLen() {
		return DecimalLen;
	}

	public void setDecimalLen(int[] decimalLen) {
		DecimalLen = decimalLen;
	}

	public boolean isBMulHead() {
		return bMulHead;
	}

	public void setBMulHead(boolean mulHead) {
		bMulHead = mulHead;
	}

	public int[] getColsHeadAlign() {
		return ColsHeadAlign;
	}

	public void setColsHeadAlign(int[] colsHeadAlign) {
		ColsHeadAlign = colsHeadAlign;
	}

	public int getTableHeadFontSize() {
		return tableHeadFontSize;
	}

	public void setTableHeadFontSize(int tableHeadFontSize) {
		this.tableHeadFontSize = tableHeadFontSize;
	}

	public int getDataFontSize() {
		return dataFontSize;
	}

	public void setDataFontSize(int dataFontSize) {
		this.dataFontSize = dataFontSize;
	}
}
