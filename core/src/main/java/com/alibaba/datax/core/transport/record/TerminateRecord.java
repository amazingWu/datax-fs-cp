package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Record;

import java.io.InputStream;

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
	public String getDirPath() {
		return null;
	}


	@Override
	public String getFileName() {
		return null;
	}

	@Override
	public void setDirPath(String filePath) {

	}

	@Override
	public void setFileInputStream(InputStream inputStream) {

	}

	@Override
	public void setFileName(String fileName) {

	}
}
