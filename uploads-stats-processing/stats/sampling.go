package stats

import (
	"math/rand"
	"sort"
)

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p int64Slice) Sort() { sort.Sort(p) }

type QuantileSample struct {
	sample []int64
	seen   int
}

func NewQuantileSample(n int) *QuantileSample {
	return &QuantileSample{
		sample: make([]int64, n),
		seen:   0,
	}
}

func (sample *QuantileSample) Append(value int64) {
	if sample.seen < len(sample.sample) {
		sample.sample[sample.seen] = value
	} else {
		index := rand.Intn(sample.seen + 1)
		if index < len(sample.sample) {
			sample.sample[index] = value
		}
	}
	sample.seen++
}

func scaleIndex(idx int, scalingRatio float64) int {
	return int(float64(idx)*scalingRatio + 0.5)
}

func (sample *QuantileSample) Quantiles() []int64 {
	sort.Sort(int64Slice(sample.sample))
	if sample.seen >= len(sample.sample) {
		return sample.sample
	}
	filledSample := make([]int64, len(sample.sample))
	scalingRatio := float64(sample.seen) / float64(len(sample.sample))
	for idx := 0; idx < len(filledSample); idx++ {
		filledSample[idx] = sample.sample[scaleIndex(idx, scalingRatio)]
	}
	return filledSample
}

func (sample *QuantileSample) Count() int {
	return sample.seen
}

func (sample *QuantileSample) Reset() {
	sample.seen = 0
}
