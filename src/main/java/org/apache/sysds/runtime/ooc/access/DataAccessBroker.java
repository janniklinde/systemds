package org.apache.sysds.runtime.ooc.access;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DataAccessBroker {
	void prioritize(AccessKey key, int priority);

	DataAvailability status(AccessKey key);

	CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> request(AccessKey key);

	CompletableFuture<List<OOCStream.QueueCallback<IndexedMatrixValue>>> request(AccessKey... key);

	interface AccessKey { }

	class TileKey implements AccessKey {
		private final long _streamId;
		private final MatrixIndexes _ix;

		public TileKey(long streamId, MatrixIndexes ix) {
			this._streamId = streamId;
			this._ix = ix;
		}

		public boolean equals(Object o) {
			return o instanceof TileKey && ((TileKey) o)._streamId == _streamId
				&& ((TileKey) o)._ix.equals(_ix);
		}

		public int hashCode() {
			return UtilFunctions.longHashCode(_streamId, _ix.getRowIndex(), _ix.getColumnIndex());
		}
	}

	class StreamKey implements AccessKey {
		private final long _streamId;
		private final long _blockIdx;

		public StreamKey(long streamId, long blockIdx) {
			this._streamId = streamId;
			this._blockIdx = blockIdx;
		}

		public boolean equals(Object o) {
			return o instanceof StreamKey && ((StreamKey) o)._streamId == _streamId
				&& ((StreamKey) o)._blockIdx == _blockIdx;
		}

		public int hashCode() {
			return UtilFunctions.longHashCode(_streamId, _blockIdx);
		}
	}
}
