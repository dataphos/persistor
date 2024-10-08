// Copyright 2024 Syntio Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package repo

import (
	"sync"
	"sync/atomic"
)

type refCounter struct {
	counter int64
	lock    *sync.RWMutex
}

type KeyMutex interface {
	Lock(interface{})
	RLock(interface{})
	Unlock(interface{})
	RUnlock(interface{})
}

type lock struct {
	inUse sync.Map
	pool  *sync.Pool
	lock  sync.Mutex
}

func NewKeyMutex() KeyMutex {
	return &lock{
		pool: &sync.Pool{
			New: func() interface{} {
				return &sync.RWMutex{}
			},
		},
	}
}

func (l *lock) Lock(key interface{}) {
	l.lock.Lock()
	defer l.lock.Unlock()
	m := l.getLocker(key)
	atomic.AddInt64(&m.counter, 1)
	m.lock.Lock()
}

func (l *lock) RLock(key interface{}) {
	l.lock.Lock()
	defer l.lock.Unlock()
	m := l.getLocker(key)
	atomic.AddInt64(&m.counter, 1)
	m.lock.RLock()
}

func (l *lock) Unlock(key interface{}) {
	m, hit := l.tryGetLocker(key)
	if !hit {
		panic("Lock should exist under key")
	}
	m.lock.Unlock()
	l.putBackInPool(key, m)
}

func (l *lock) RUnlock(key interface{}) {
	m, hit := l.tryGetLocker(key)
	if !hit {
		panic("Lock should exist under key")
	}
	m.lock.RUnlock()
	l.putBackInPool(key, m)
}

func (l *lock) putBackInPool(key interface{}, m *refCounter) {
	l.lock.Lock()
	defer l.lock.Unlock()
	atomic.AddInt64(&m.counter, -1)

	if m.counter <= 0 {
		l.pool.Put(m.lock)
		l.inUse.Delete(key)
	}
}

func (l *lock) getLocker(key interface{}) *refCounter {
	res, _ := l.inUse.LoadOrStore(key, &refCounter{
		counter: 0,
		lock:    l.pool.Get().(*sync.RWMutex),
	})

	return res.(*refCounter)
}
func (l *lock) tryGetLocker(key interface{}) (*refCounter, bool) {
	res, ok := l.inUse.Load(key)
	return res.(*refCounter), ok
}
