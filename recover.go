package ipc

// A stall is a claim that stops the reader at head: its producer has not
// written the header yet, or has not committed it.
type stall struct {
	at     uint64
	length int32
	// slots holds every slot whose intent covers at.
	slots []int
}

// stalled reports the claim that stops the reader, if there is one.
func (r *Ring) stalled() (stall, bool) {
	head := r.hdr.head.Load()
	if head == r.hdr.tail.Load() {
		return stall{}, false
	}
	length := r.loadLength(head & r.mask)
	if length > 0 {
		return stall{}, false
	}
	st := stall{at: head, length: length}
	for idx := range r.hdr.slots {
		slot := &r.hdr.slots[idx]
		at := slot.at.Load()
		if at != noIntent && at <= head && head < at+slot.size.Load() {
			st.slots = append(st.slots, idx)
		}
	}
	return st, true
}

// reclaim turns a stalled claim into padding, up to end. A dead producer made
// the claim, and end is where that claim stops. A false return means that the
// claim changed under the reader.
func (r *Ring) reclaim(st stall, end uint64, dead []int) bool {
	index := st.at & r.mask
	size := end - st.at
	// A claim that crossed the wrap point is reclaimed one lap segment at a
	// time.
	if toEnd := uint64(r.Capacity()) - index; size > toEnd {
		size = toEnd
	}
	// Only the reader reads a header, and the reader is this goroutine, so the
	// type can follow the length.
	if !r.casLength(index, st.length, int32(size)) {
		return false
	}
	r.storeType(index, TypePadding)
	if st.at+size == end {
		for _, idx := range dead {
			r.hdr.slots[idx].at.Store(noIntent)
		}
	}
	return true
}

// acquireSlot takes a claim slot for self. It prefers a free slot. Otherwise
// it takes a slot whose owner is dead and whose claim stops the reader.
func (r *Ring) acquireSlot(self procID, ns uint64, dead func(procID) bool) (int, error) {
	for idx := range r.hdr.slots {
		if r.hdr.slots[idx].owner.CompareAndSwap(0, uint64(self)) {
			r.hdr.slots[idx].ns.Store(ns)
			r.hdr.slots[idx].at.Store(noIntent)
			return idx, nil
		}
	}
	head := r.hdr.head.Load()
	for idx := range r.hdr.slots {
		slot := &r.hdr.slots[idx]
		owner := procID(slot.owner.Load())
		if owner == noProc || owner == self {
			continue
		}
		if at := slot.at.Load(); at != noIntent && at+slot.size.Load() > head {
			continue
		}
		if slot.ns.Load() != ns || !dead(owner) {
			continue
		}
		if slot.owner.CompareAndSwap(uint64(owner), uint64(self)) {
			slot.ns.Store(ns)
			slot.at.Store(noIntent)
			return idx, nil
		}
	}
	return -1, ErrTooManyClaims
}

// dropSlot returns a slot to the shared pool.
func (r *Ring) dropSlot(self procID, slot int) {
	r.hdr.slots[slot].at.Store(noIntent)
	r.hdr.slots[slot].owner.CompareAndSwap(uint64(self), 0)
}
