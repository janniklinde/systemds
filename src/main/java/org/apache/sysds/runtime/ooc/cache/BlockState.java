package org.apache.sysds.runtime.ooc.cache;

public enum BlockState {
	HOT,
	WARM,
	EVICTING,
	READING,
	//DEFERRED_READ, // Deferred read
	COLD,
	REMOVED; // Removed state means that it is not owned by the cache anymore. It doesn't mean the object is dereferenced

	public boolean isAvailable() {
		return this == HOT || this == WARM || this == EVICTING || this == REMOVED;
	}

	public boolean isUnavailable() {
		return this == COLD || this == READING;
	}

	public boolean readScheduled() {
		return this == READING;
	}

	public boolean isBackedByDisk() {
		return switch(this) {
			case WARM, COLD, READING -> true;
			default -> false;
		};
	}
}
