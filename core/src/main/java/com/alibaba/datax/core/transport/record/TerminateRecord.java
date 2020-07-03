package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Record;

import java.io.InputStream;
import java.util.List;

/**
 * 作为标示 生产者已经完成生产的标志
 * 
 */
public class TerminateRecord implements Record {
	private final static TerminateRecord SINGLE = new TerminateRecord();

	private TerminateRecord() {
	}

	public static TerminateRecord get() {
		return SINGLE;
	}

	@Override
	public InputStream getInputStream() {
		return null;
	}

	@Override
	public String getFileMd5() {
		return null;
	}

	@Override
	public void setFileMd5(String md5) {

	}

	@Override
	public void setFileLength(Long fileLength) {

	}

	@Override
	public Long getFileLength() {
		return 0L;
	}

	@Override
	public List<String> getDirPath() {
		return null;
	}


	@Override
	public String getFileName() {
		return null;
	}

	@Override
	public void setDirPath(List<String> filePath) {

	}

	@Override
	public void setFileInputStream(InputStream inputStream) {

	}

	@Override
	public void setFileName(String fileName) {

	}

	@Override
	public void destroy() {
	}
}
