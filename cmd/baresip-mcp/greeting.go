package main

import (
	"encoding/binary"
	"math"
	"os"
)

// writeRingbackWAV writes a single period of EU ringback (1 s of a
// 425 Hz tone + 3 s silence) as 16-bit PCM mono 8 kHz WAV. baresip
// loops the file while the callee rings, so a single cadence period is
// all we need.
func writeRingbackWAV(path string) error {
	return writePCMWAV(path, func(add func(float64, int, float64), addSilence func(int)) {
		add(425, 1000, 8000)
		addSilence(3000)
	})
}

// writeGreetingWAV writes a short two-tone greeting followed by silence
// to path as 16-bit PCM mono 8 kHz WAV. baresip's aufile audio source
// plays the file once when the call is established; after the greeting
// finishes the source stops and the caller hears silence.
func writeGreetingWAV(path string) error {
	return writePCMWAV(path, func(addTone func(float64, int, float64), addSilence func(int)) {
		// "ding-ding" pattern: 880Hz then 1320Hz, comfortable volume.
		addTone(880, 180, 6000)
		addSilence(80)
		addTone(1320, 180, 6000)
		addSilence(200)
	})
}

// writePCMWAV builds a sample buffer via the build callback and writes
// it as 8 kHz mono 16-bit PCM WAV at path.
func writePCMWAV(path string, build func(addTone func(freq float64, ms int, amp float64), addSilence func(ms int))) error {
	const sampleRate = 8000

	var samples []int16
	addTone := func(freq float64, ms int, amp float64) {
		n := sampleRate * ms / 1000
		for i := 0; i < n; i++ {
			v := math.Sin(2 * math.Pi * freq * float64(i) / float64(sampleRate))
			// Linear fade in/out to avoid clicks (5 ms ramp on each side).
			ramp := sampleRate * 5 / 1000
			gain := 1.0
			if i < ramp {
				gain = float64(i) / float64(ramp)
			} else if i > n-ramp {
				gain = float64(n-i) / float64(ramp)
			}
			samples = append(samples, int16(v*amp*gain))
		}
	}
	addSilence := func(ms int) {
		n := sampleRate * ms / 1000
		for i := 0; i < n; i++ {
			samples = append(samples, 0)
		}
	}

	build(addTone, addSilence)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dataBytes := len(samples) * 2
	// RIFF header.
	if _, err := f.Write([]byte("RIFF")); err != nil {
		return err
	}
	_ = binary.Write(f, binary.LittleEndian, uint32(36+dataBytes))
	if _, err := f.Write([]byte("WAVEfmt ")); err != nil {
		return err
	}
	_ = binary.Write(f, binary.LittleEndian, uint32(16)) // PCM header size
	_ = binary.Write(f, binary.LittleEndian, uint16(1))  // PCM format
	_ = binary.Write(f, binary.LittleEndian, uint16(1))  // mono
	_ = binary.Write(f, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(f, binary.LittleEndian, uint32(sampleRate*2)) // byte rate
	_ = binary.Write(f, binary.LittleEndian, uint16(2))            // block align
	_ = binary.Write(f, binary.LittleEndian, uint16(16))           // bits per sample
	if _, err := f.Write([]byte("data")); err != nil {
		return err
	}
	_ = binary.Write(f, binary.LittleEndian, uint32(dataBytes))
	return binary.Write(f, binary.LittleEndian, samples)
}
