// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"sync"
	"testing"
)

// TestClusterCloseDoesNotRaceWithBatchReads reproduces the production crash
//
//	fatal error: concurrent map read and map write
//	...GetNodeBatchRead(...)/partition.go
//
// Cluster.Close() used to mutate the *live* partition map in place
// (partitionMap.cleanup: delete(pm, ns) + nilling the Replicas slices) while
// in-flight operations were still reading that same map via
// cluster.getPartitions(). Every other partition-map mutation in the client
// respects copy-on-write (clone, mutate the clone, atomically swap the
// reference), so a reader that already holds a snapshot is safe. cleanup() was
// the sole exception and therefore the sole source of the data race.
//
// Run with -race to catch it deterministically; without -race the Go runtime's
// concurrent-map detector will hard-fatal the process, which is the exact
// production symptom.
func TestClusterCloseDoesNotRaceWithBatchReads(t *testing.T) {
	cluster := &Cluster{tendChannel: make(chan struct{})}

	pm := make(partitionMap)
	for _, ns := range []string{"ns0", "ns1", "ns2", "ns3"} {
		pm[ns] = newPartitions(_PARTITIONS, 2, false)
	}
	cluster.partitionWriteMap.Set(pm)

	key, err := NewKey("ns1", "set", 1)
	if err != nil {
		t.Fatalf("NewKey: %s", err)
	}

	const readers = 8
	var wg sync.WaitGroup
	started := make(chan struct{})
	var once sync.Once

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			once.Do(func() { close(started) })
			// Hammer the exact read path from the crash stack. The map read at
			// pmap[key.namespace] is what races with Close's map write.
			for j := 0; j < 5000; j++ {
				_, _ = GetNodeBatchRead(cluster, key, SEQUENCE, SEQUENCE, nil, 0, 0)
			}
		}()
	}

	// Ensure readers are live before closing so the read and write windows
	// overlap.
	<-started
	cluster.Close()

	wg.Wait()
}
