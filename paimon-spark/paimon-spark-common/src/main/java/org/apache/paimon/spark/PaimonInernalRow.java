package org.apache.paimon.spark;

import org.apache.paimon.data.*;
import org.apache.paimon.types.RowKind;

public class PaimonInernalRow implements InternalRow {

    RowKind rowKind;
    org.apache.spark.sql.catalyst.InternalRow internalRow;

    public PaimonInernalRow(org.apache.spark.sql.catalyst.InternalRow internalRow) {
        this.internalRow = internalRow;
    }


    public PaimonInernalRow(RowKind rowKind, org.apache.spark.sql.catalyst.InternalRow internalRow) {
        this.rowKind = rowKind;
        this.internalRow = internalRow;
    }

    @Override
    public boolean isNullAt(int pos) {
        return internalRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return internalRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return internalRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return internalRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return internalRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return internalRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return internalRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return internalRow.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromString(internalRow.getString(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return Decimal.fromBigDecimal(internalRow.getDecimal(pos, precision, scale).toBigDecimal().bigDecimal(), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return Timestamp.fromEpochMillis(internalRow.getLong(pos));
    }

    @Override
    public byte[] getBinary(int pos) {
        return internalRow.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return (InternalArray) internalRow.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return null;
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new PaimonInernalRow(internalRow.getStruct(pos, numFields));
    }

    @Override
    public int getFieldCount() {
        return internalRow.numFields();
    }

    @Override
    public RowKind getRowKind() {
        return this.rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }
}
