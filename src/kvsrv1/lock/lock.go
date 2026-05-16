package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	id       string
	// version rpc.Tversion
}

const unlock = "unlock"

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, id: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		lid, lver, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey || (err == rpc.OK && lid == unlock) {
			err = lk.ck.Put(lk.lockname, lk.id, lver)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				lid, lver, err = lk.ck.Get(lk.lockname)
				if err == rpc.OK && lid == lk.id {
					return
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	lid, ver, err := lk.ck.Get(lk.lockname)
	if err == rpc.OK && lid == lk.id {
		err = lk.ck.Put(lk.lockname, unlock, ver)
	}
}
