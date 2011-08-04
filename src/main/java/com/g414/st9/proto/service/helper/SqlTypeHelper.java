package com.g414.st9.proto.service.helper;

import com.g414.st9.proto.service.schema.AttributeType;

public interface SqlTypeHelper {
	public String getSqlType(AttributeType type);

	public String getInsertIgnore();

	public String getPKConflictResolve();

	public String quote(String name);
}
