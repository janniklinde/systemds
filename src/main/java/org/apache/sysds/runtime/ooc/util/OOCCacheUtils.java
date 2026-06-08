/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.ooc.util;

import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.legacy.OOCCacheScheduler;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.lang.ref.SoftReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class OOCCacheUtils {
	private static final long HANDOVER_STREAM_ID = CachingStream._streamSeq.getNextID();
	private static final AtomicLong HANDOVER_BLOCK_ID = new AtomicLong();

	public record TileHandle(BlockKey key, long bytes, Kind kind)
		implements AutoCloseable {
		public enum Kind {
			CACHE,
			BACKEND
		}

		public CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> read(MemoryAllowance owner) {
			if(kind() == Kind.CACHE)
				return OOCCacheManager.requestBlockBacked(key(), owner, bytes());

			return OOCCacheManager.getTileStoreBackend().read(key())
				.thenApply(imv -> imv == null ? null : new InMemoryQueueCallback(imv, null, owner, bytes()));
		}

		@Override
		public void close() {
			if(kind() == Kind.CACHE)
				OOCCacheManager.getCache().forget(key());
			else
				OOCCacheManager.getTileStoreBackend().delete(key());
		}
	}

	public static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> handover(
		OOCStream.QueueCallback<IndexedMatrixValue> cb, MemoryAllowance allowance) {
		if(cb == null)
			return CompletableFuture.completedFuture(null);

		try {
			return CompletableFuture.completedFuture(cb.transferOwnershipBlocking(allowance));
		}
		catch(UnsupportedOperationException ex) {
			return handoverBySpillAndRead(cb, allowance, true);
		}
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> tryHandover(
		OOCStream.QueueCallback<IndexedMatrixValue> cb, MemoryAllowance allowance) {
		try {
			OOCStream.QueueCallback<IndexedMatrixValue> transferred = cb.tryTransferOwnership(allowance);
			if(transferred != null)
				return transferred;
		}
		catch(UnsupportedOperationException ex) {
			return null;
		}
		return null;
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> retainLocal(
		OOCStream.QueueCallback<IndexedMatrixValue> cb) {
		if(cb == null)
			return null;
		BlockKey key = cb.getBlockKey();
		if(key == null)
			return cb;
		try {
			OOCCacheManager.getCache().addReference(key);
			return new ForgettableCallback<>(cb, new ForgetHandle(key));
		}
		catch(RuntimeException ex) {
			cb.close();
			throw ex;
		}
	}

	public static void noteEscape(OOCStream.QueueCallback<IndexedMatrixValue> cb, String escape) {
		cb = unwrap(cb);
		if(cb instanceof InMemoryQueueCallback inMemory)
			InMemoryQueueCallback.noteEscape(inMemory, escape);
		else if(cb != null && cb.getBackingPin() != null)
			OOCCacheManager.noteBackedEscape(cb, escape);
	}

	public static CompletableFuture<TileHandle> spill(OOCStream.QueueCallback<IndexedMatrixValue> cb, BlockKey targetKey) {
		return spill(cb, targetKey, cb.getManagedBytes());
	}

	public static CompletableFuture<TileHandle> spill(OOCStream.QueueCallback<IndexedMatrixValue> cb, BlockKey targetKey,
		long logicalBytes) {
		if(cb == null)
			return CompletableFuture.completedFuture(null);
		if(logicalBytes <= 0)
			throw new IllegalArgumentException("Logical bytes must be positive for spill.");

		cb = unwrap(cb);
		OOCCacheScheduler.AllowanceBackedPin pin = cb.getBackingPin();
		if(pin != null)
			return spillBacked(pin, logicalBytes);

		if(cb instanceof InMemoryQueueCallback) {
			OOCStream.QueueCallback<IndexedMatrixValue> retained = cb.keepOpen();
			try {
				IndexedMatrixValue imv = retained.get();
				SoftReference<IndexedMatrixValue> softLocal = new SoftReference<>(imv);
				return OOCCacheManager.getTileStoreBackend().spill(targetKey, imv)
					.handle((ignored, ex) -> {
						try {
							if(ex != null)
								throw ex instanceof RuntimeException ? (RuntimeException) ex :
									new RuntimeException(ex);
							return new TileHandle(targetKey, logicalBytes, TileHandle.Kind.BACKEND);
						}
						finally {
							retained.close();
						}
					});
			}
			catch(RuntimeException ex) {
				retained.close();
				throw ex;
			}
		}

		throw new IllegalArgumentException("Unsupported callback type for spill: " + cb.getClass().getName());
	}

	public static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> read(TileHandle handle,
		MemoryAllowance owner) {
		if(handle == null)
			return CompletableFuture.completedFuture(null);
		return handle.read(owner);
	}

	private static CompletableFuture<TileHandle> spillBacked(OOCCacheScheduler.AllowanceBackedPin pin, long logicalBytes) {
		OOCCacheScheduler.BackingReleaseHandle release = OOCCacheManager.getCache().releaseBacking(pin.keepOpen());
		CompletableFuture<Boolean> future = release.getCompletionFuture();
		if(release.isCommitted())
			return CompletableFuture.completedFuture(createCacheHandle(release.getKey(), logicalBytes));

		return future.thenApply(committed -> {
			if(committed)
				return createCacheHandle(release.getKey(), logicalBytes);
			OOCCacheScheduler.AllowanceBackedPin reclaimed = release.reclaim();
			if(reclaimed != null)
				reclaimed.close();
			throw new IllegalStateException("Cache-backed spill was cancelled before commit for key " + release.getKey());
		});
	}

	private static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> handoverBySpillAndRead(
		OOCStream.QueueCallback<IndexedMatrixValue> cb, MemoryAllowance allowance, boolean blockingReserve) {
		long bytes = cb.getManagedBytes();
		if(bytes <= 0)
			return CompletableFuture.completedFuture(cb);

		boolean reservedEarly = !blockingReserve;
		if(reservedEarly && !tryReserveForReadback(allowance, bytes))
			return CompletableFuture.completedFuture(null);

		BlockKey targetKey = cb.getBlockKey() != null ? cb.getBlockKey() : nextHandoverKey();
		return spill(cb, targetKey, bytes)
			.thenCompose(handle -> {
				closeSourceAfterSpill(cb);
				if(blockingReserve) {
					try {
						reserveBlockingForReadback(allowance, handle.bytes());
					}
					catch(RuntimeException ex) {
						handle.close();
						throw ex;
					}
				}
				return read(handle, allowance)
					.whenComplete((readCb, ex) -> cleanupReadback(handle, allowance, readCb, ex));
			})
			.whenComplete((ignored, ex) -> {
				if(ex != null && reservedEarly)
					allowance.release(bytes);
			});
	}

	private static void reserveBlockingForReadback(MemoryAllowance allowance, long bytes) {
		if(allowance instanceof CachedAllowance cached)
			cached.admitBlocking(bytes);
		else
			allowance.reserveBlocking(bytes);
	}

	private static boolean tryReserveForReadback(MemoryAllowance allowance, long bytes) {
		if(allowance instanceof CachedAllowance)
			return false;
		return allowance.tryReserve(bytes);
	}

	private static void closeSourceAfterSpill(OOCStream.QueueCallback<IndexedMatrixValue> cb) {
		if(cb != null) {
			cb.close();
			cb.forget();
		}
	}

	private static void cleanupReadback(TileHandle handle, MemoryAllowance allowance,
		OOCStream.QueueCallback<IndexedMatrixValue> readCb, Throwable ex) {
		if(ex != null || readCb == null)
			allowance.release(handle.bytes());
		handle.close();
	}

	private static BlockKey nextHandoverKey() {
		return new BlockKey(HANDOVER_STREAM_ID, HANDOVER_BLOCK_ID.getAndIncrement());
	}

	private static TileHandle createCacheHandle(BlockKey key, long logicalBytes) {
		OOCCacheManager.getCache().addReference(key);
		return new TileHandle(key, logicalBytes, TileHandle.Kind.CACHE);
	}

	@SuppressWarnings("unchecked")
	private static <T> OOCStream.QueueCallback<T> unwrap(OOCStream.QueueCallback<T> cb) {
		return cb instanceof ForgettableCallback<?> wrapped ? (OOCStream.QueueCallback<T>) wrapped._delegate : cb;
	}

	private static final class ForgetHandle {
		private final BlockKey _key;
		private final AtomicBoolean _forgotten = new AtomicBoolean(false);

		private ForgetHandle(BlockKey key) {
			_key = key;
		}

		private void forget() {
			if(_forgotten.compareAndSet(false, true))
				OOCCacheManager.getCache().forget(_key);
		}
	}

	private static final class ForgettableCallback<T> implements OOCStream.QueueCallback<T> {
		private final OOCStream.QueueCallback<T> _delegate;
		private final ForgetHandle _handle;

		private ForgettableCallback(OOCStream.QueueCallback<T> delegate, ForgetHandle handle) {
			_delegate = delegate;
			_handle = handle;
		}

		@Override
		public T get() {
			return _delegate.get();
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			return new ForgettableCallback<>(_delegate.keepOpen(), _handle);
		}

		@Override
		public void close() {
			_delegate.close();
		}

		@Override
		public void fail(org.apache.sysds.runtime.DMLRuntimeException failure) {
			_delegate.fail(failure);
		}

		@Override
		public boolean isEos() {
			return _delegate.isEos();
		}

		@Override
		public boolean isFailure() {
			return _delegate.isFailure();
		}

		@Override
		public long getManagedBytes() {
			return _delegate.getManagedBytes();
		}

		@Override
		public OOCStream.QueueCallback<T> transferOwnershipBlocking(MemoryAllowance allowance) {
			return new ForgettableCallback<>(_delegate.transferOwnershipBlocking(allowance), _handle);
		}

		@Override
		public OOCStream.QueueCallback<T> tryTransferOwnership(MemoryAllowance allowance) {
			OOCStream.QueueCallback<T> transferred = _delegate.tryTransferOwnership(allowance);
			return transferred == null ? null : new ForgettableCallback<>(transferred, _handle);
		}

		@Override
		public BlockKey getBlockKey() {
			return _delegate.getBlockKey();
		}

		@Override
		public OOCCacheScheduler.AllowanceBackedPin getBackingPin() {
			return _delegate.getBackingPin();
		}

		@Override
		public void forget() {
			_handle.forget();
		}
	}
}
