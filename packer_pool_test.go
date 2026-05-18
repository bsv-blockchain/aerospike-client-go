// Copyright 2024- bsv-blockchain
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aerospike

import (
	"bytes"
	"sync"
	"testing"
)

// TestPackerPool_RoundTrip exercises the packer pool's safety contract:
// after BytesAndPut returns, the returned slice must own its own backing
// array, and a subsequent newPacker() may legally reuse the underlying
// bytes.Buffer (which means the next user's Write/Reset must not corrupt
// previously-returned slices).
func TestPackerPool_RoundTrip(t *testing.T) {
	p1 := newPacker()
	for i := 0; i < 100; i++ {
		p1.WriteByte(byte(i))
	}
	out1 := p1.BytesAndPut()
	if len(out1) != 100 {
		t.Fatalf("out1 len = %d, want 100", len(out1))
	}
	for i := 0; i < 100; i++ {
		if out1[i] != byte(i) {
			t.Fatalf("out1[%d] = %d, want %d", i, out1[i], i)
		}
	}

	// Second user. If the pool handed us p1 back, its Buffer was Reset()
	// so any new writes start fresh; we should not see any of p1's
	// previous bytes in the new Bytes() output.
	p2 := newPacker()
	for i := 0; i < 50; i++ {
		p2.WriteByte(byte(200 - i))
	}
	out2 := p2.BytesAndPut()
	if len(out2) != 50 {
		t.Fatalf("out2 len = %d, want 50", len(out2))
	}
	for i := 0; i < 50; i++ {
		if out2[i] != byte(200-i) {
			t.Fatalf("out2[%d] = %d, want %d", i, out2[i], byte(200-i))
		}
	}

	// Critically: out1 must still contain its original bytes even after
	// p2 reused the pool. If BytesAndPut had aliased Buffer.Bytes
	// instead of copying, out1 would be partially overwritten with
	// out2's contents.
	for i := 0; i < 100; i++ {
		if out1[i] != byte(i) {
			t.Fatalf("out1[%d] corrupted after pool reuse: got %d, want %d", i, out1[i], i)
		}
	}
}

// TestPackerPool_LargeBufferReuse drives a single pool slot through enough
// growth cycles that the underlying bytes.Buffer reaches steady-state
// capacity. After the first few rounds no further allocation should be
// needed by the buffer itself; we cannot assert allocs from outside
// reliably across Go versions, so we instead assert correctness across
// many round trips at varied sizes.
func TestPackerPool_LargeBufferReuse(t *testing.T) {
	sizes := []int{16, 512, 8192, 64, 4096, 32, 16384, 1, 8, 256}
	for round := 0; round < 4; round++ {
		for _, sz := range sizes {
			p := newPacker()
			expected := make([]byte, sz)
			for i := 0; i < sz; i++ {
				b := byte((round*7 + i) & 0xff)
				expected[i] = b
				p.WriteByte(b)
			}
			got := p.BytesAndPut()
			if !bytes.Equal(got, expected) {
				t.Fatalf("round=%d sz=%d: bytes mismatch", round, sz)
			}
		}
	}
}

// TestPackerPool_ConcurrentUse hammers the pool with many goroutines to
// surface any data race (run with -race).
func TestPackerPool_ConcurrentUse(t *testing.T) {
	const goroutines = 32
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				p := newPacker()
				sz := 1 + ((g*iters + i) % 1024)
				for j := 0; j < sz; j++ {
					p.WriteByte(byte(j))
				}
				out := p.BytesAndPut()
				if len(out) != sz {
					t.Errorf("len = %d, want %d", len(out), sz)
					return
				}
				for j := 0; j < sz; j++ {
					if out[j] != byte(j) {
						t.Errorf("byte mismatch at %d", j)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}
