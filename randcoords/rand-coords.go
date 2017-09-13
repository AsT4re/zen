package randcoords

import (
	"math"
	"math/rand"
)

/*
 * Return a random float64 in range [min, max) with a precision of 7 decimals (enough for coordinates)
*/
func GetRandCoord(r *rand.Rand, min, max float64) float64 {
	return toFixed(r.Float64() * (max - min) + min, 7)
}

/*
 * Returns bounds min max with a certain variation delta
 * from a given float64 value in range [min, max).
 * Bounds cannot exceed min max
*/
func GetBounds(r *rand.Rand, min, max, delta float64) (float64, float64) {
	rnd := GetRandCoord(r, min, max)
	bndMin := rnd - delta
	if bndMin < min {
		bndMin = min
	}

	bndMax := rnd + delta
	if bndMax > max {
		bndMax = max
	}

	return bndMin, bndMax
}

func toFixed(val float64, prec int) float64 {
	mult := math.Pow(10, float64(prec))
	return float64(int(val * mult)) / mult
}
