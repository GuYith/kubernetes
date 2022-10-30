/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package debugger

import (
	"os"
	"os/signal"

	corelisters "k8s.io/client-go/listers/core/v1"
	internalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
)

// CacheDebugger provides ways to check and write cache information for debugging.
type CacheDebugger struct {
	Comparer CacheComparer
	Dumper   CacheDumper
}

// New creates a CacheDebugger.
func New(
	nodeLister corelisters.NodeLister,
	podLister corelisters.PodLister,
	cache internalcache.Cache,
	podQueue internalqueue.SchedulingQueue,
) *CacheDebugger {
	return &CacheDebugger{
		Comparer: CacheComparer{
			NodeLister: nodeLister,
			PodLister:  podLister,
			Cache:      cache,
			PodQueue:   podQueue,
		},
		Dumper: CacheDumper{
			cache:    cache,
			podQueue: podQueue,
		},
	}
}

// ListenForSignal starts a goroutine that will trigger the CacheDebugger's
// behavior when the process receives SIGINT (Windows) or SIGUSER2 (non-Windows).  ListenForSignal启动一个goroutine协程，当进程收到SIGINT (Windows)或SIGUSER2(非Windows)时，该协程将触发CacheDebugger的行为。
func (d *CacheDebugger) ListenForSignal(stopCh <-chan struct{}) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, compareSignal) //传信号到ch

	go func() { //启动协程函数
		for {
			select { //监听IO操作
			case <-stopCh: //stopCh 传出数据
				return
			case <-ch: //若收到消息，将触发CacheDebugger的行为
				d.Comparer.Compare()
				d.Dumper.DumpAll()
			}
		}
	}()
}
