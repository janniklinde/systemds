package org.apache.sysds.runtime.ooc.cache;

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;

public final class BlockEntry {
	private final BlockKey _key;
	private final long _size;
	private volatile int _pinCount;
	private volatile BlockState _state;
	private Object _data;

	BlockEntry(BlockKey key, long size, Object data) {
		this._key = key;
		this._size = size;
		this._pinCount = 0;
		this._state = BlockState.HOT;
		this._data = data;
	}

	public BlockKey getKey() {
		return _key;
	}

	public long getSize() {
		return _size;
	}

	public Object getData() {
		if (_pinCount > 0)
			return _data;
		throw new IllegalStateException("Cannot get the data of an unpinned entry");
	}

	Object getDataUnsafe() {
		return _data;
	}

	void setDataUnsafe(Object data) {
		_data = data;
	}

	public BlockState getState() {
		return _state;
	}

	public boolean isPinned() {
		return _pinCount > 0;
	}

	synchronized void setState(BlockState state) {
		_state = state;
	}

	/**
	 * Tries to clear the underlying data if it is not pinned
	 * @return the number of cleared bytes (or 0 if could not clear or data was already cleared)
	 */
	synchronized long clear() {
		if (_pinCount != 0 || _data == null)
			return 0;
		if (_data instanceof IndexedMatrixValue)
			((IndexedMatrixValue)_data).setValue(null); // Explicitly clear
		_data = null;
		return _size;
	}

	/**
	 * Pins the underlying data in memory
	 * @return the new number of pins (0 if pin was unsuccessful)
	 */
	synchronized int pin() {
		if (_data == null)
			return 0;
		_pinCount++;
		return _pinCount;
	}

	/**
	 * Unpins the underlying data
	 * @return true if the data is now unpinned
	 */
	synchronized boolean unpin() {
		if (_pinCount <= 0)
			throw new IllegalStateException("Cannot unpin data if it was not pinned");
		_pinCount--;
		return _pinCount == 0;
	}
}
