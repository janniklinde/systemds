package org.apache.sysds.runtime.ooc.util;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

public class OOCPrimitiveUtils {
	public static CompletableFuture<Void> collect(OOCStream<IndexedMatrixValue> stream, CachedAllowance cache,
		ToIntFunction<MatrixIndexes> fn) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		new Thread(() -> {
			try {
				OOCStream.QueueCallback<IndexedMatrixValue> cb;

				while((cb = stream.dequeueCB()) != null && !cb.isEos()) {
					try {
						IndexedMatrixValue imv = cb.get();
						int idx = fn.applyAsInt(imv.getIndexes());
						cache.handover(cb.keepOpen(), idx);
					}
					finally {
						cb.close();
					}
				}
				if(cb != null)
					cb.close();
				future.complete(null);
			}
			catch(Throwable t) {
				future.completeExceptionally(DMLRuntimeException.of(t));
			}
		}).start();
		return future;
	}

	public static CompletableFuture<Void> collect(OOCStream<IndexedMatrixValue> stream, CachedAllowance cache,
		ToIntFunction<MatrixIndexes> fn, OOCStream<IndexedMatrixValue> workStream) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		new Thread(() -> {
			try {
				OOCStream.QueueCallback<IndexedMatrixValue> cb;

				while((cb = stream.dequeueCB()) != null && !cb.isEos()) {
					try {
						IndexedMatrixValue imv = cb.get();
						int idx = fn.applyAsInt(imv.getIndexes());
						cache.handover(cb.keepOpen(), idx);
						workStream.enqueue(cb.keepOpen());
					}
					finally {
						cb.close();
					}
				}
				if(cb != null)
					cb.close();
				workStream.closeInput();
				future.complete(null);
			}
			catch(Throwable t) {
				DMLRuntimeException re = DMLRuntimeException.of(t);
				workStream.propagateFailure(re);
				future.completeExceptionally(re);
			}
		}).start();
		return future;
	}

	public static int accumulate(OOCStream.QueueCallback<IndexedMatrixValue> cb,
		BiFunction<OOCStream.QueueCallback<IndexedMatrixValue>, OOCStream.QueueCallback<IndexedMatrixValue>, OOCStream.QueueCallback<IndexedMatrixValue>> mergeFn,
		CachedAllowance cache, int idx) {
		OOCStream.QueueCallback<IndexedMatrixValue> candidate = cb;
		int reductions = 0;
		try {
			while(candidate != null) {
				OOCStream.QueueCallback<IndexedMatrixValue> existing = cache.handoverOrTakeExisting(candidate, idx)
					.join();
				if(existing == null) {
					candidate = null;
					return reductions;
				}

				OOCStream.QueueCallback<IndexedMatrixValue> merged;
				try(existing) {
					merged = mergeFn.apply(existing, candidate);
					reductions++;
				}
				finally {
					candidate.close();
				}
				candidate = merged;
			}
			return reductions;
		}
		catch(RuntimeException ex) {
			throw DMLRuntimeException.of(ex);
		}
		finally {
			if(candidate != null)
				candidate.close();
		}
	}
}
