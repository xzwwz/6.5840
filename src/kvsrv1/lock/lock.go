package lock

import (
	"encoding/json"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key   string
	id    string
	start time.Time
}

var DefaultTimeout = time.Second * 5

type Value struct {
	State    string          `json:"s"` //锁的状态
	Queue    []string        `json:"q"` //等待队列
	Map      map[string]bool `json:"m"` //map,用于查询client是否已经加入等待队列
	Locktime time.Time
}

// 将string转为value
func parserJson(jsonStr string) (Value, error) {
	var value Value
	err := json.Unmarshal([]byte(jsonStr), &value)
	return value, err
}

// 将value转为string
func (v *Value) toStr() string {
	jsonByte, _ := json.Marshal(v)
	return string(jsonByte)
}

func (v *Value) isLocked() bool {
	return v.State == "l"
}

func (v *Value) setLock() {
	v.State = "l"
}

func (v *Value) setUnlock() {
	v.State = "u"
}

func (v *Value) inTurn(id string) bool {
	return len(v.Queue) > 0 && v.Queue[0] == id
}

func (v *Value) containsId(id string) bool {
	_, exist := v.Map[id]
	return exist
}

func (v *Value) push(id string) {
	// index := len(v.Queue)
	v.Map[id] = true
	v.Queue = append(v.Queue, id)
}

func (v *Value) pop() {
	if len(v.Queue) > 0 {
		delete(v.Map, v.Queue[0])
		v.Queue = v.Queue[1:]
	}
}

func (v *Value) setLocktime() {
	v.Locktime = time.Now()
}

func (v *Value) timeout() bool {
	return time.Since(v.Locktime) > DefaultTimeout
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.key = l
	lk.id = kvtest.RandValue(8)

	backoff := time.Millisecond * 10
	maxbackoff := time.Millisecond * 50
	// 不断循环 get ，put 确保返回正确的 lock
	for {
		_, _, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			v := Value{
				State: "u",
				Queue: []string{},
				Map:   make(map[string]bool),
			}
			if err := lk.ck.Put(lk.key, v.toStr(), 0); err == rpc.OK {
				// fmt.Println("make lock success put lock")
				return lk
			}
		} else {
			// fmt.Println("make lock success get lock")
			return lk
		}
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		time.Sleep(backoff + jitter)
		backoff = min(backoff*2, maxbackoff)
	}
}

func (lk *Lock) Acquire() {
	// Your code here
	backoff := time.Millisecond * 10
	maxbackoff := time.Millisecond * 50
	for {
		vStr, version, e := lk.ck.Get(lk.key)
		if e != rpc.OK {
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			time.Sleep(backoff + jitter)
			backoff = min(backoff*2, maxbackoff)
			continue
		}
		v, _ := parserJson(vStr)
		// fmt.Println("get", v.Queue)
		if !v.containsId(lk.id) {
			v.push(lk.id)
			// fmt.Println("try put", lk.id)
			lk.ck.Put(lk.key, v.toStr(), version)
			continue
		}
		if v.inTurn(lk.id) {
			if !v.isLocked() || v.timeout() {
				v.setLock()
				v.pop()
				v.setLocktime()
				if err := lk.ck.Put(lk.key, v.toStr(), version); err == rpc.OK {
					return
				}
			} else {
				time.Sleep(backoff)
			}
		} else {
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			time.Sleep(backoff + jitter)
			backoff = min(backoff*2, maxbackoff)
		}

	}

}

func (lk *Lock) Release() {
	// Your code here
	backoff := time.Millisecond * 10
	maxbackoff := time.Millisecond * 50
	for {
		vStr, version, e := lk.ck.Get(lk.key)
		if e != rpc.OK {
			continue
		}
		v, _ := parserJson(vStr)
		v.setUnlock()

		if err := lk.ck.Put(lk.key, v.toStr(), version); err == rpc.OK {
			return
		} else {
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			time.Sleep(backoff + jitter)
			backoff = min(backoff*2, maxbackoff)
		}
	}

}
