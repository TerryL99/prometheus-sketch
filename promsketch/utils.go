package promsketch

const WINDOW_SIZE int = 1000000
const HASH_SEED int = 2147483647

/* sketch configurations */
const CM_ROW_NO int = 5
const CM_COL_NO int = 1000

const CS_ROW_NO int = 5
const CS_COL_NO int = 4096
const CS_ONE_COL_NO int = 100000
const CS_LVLS int = 16
const TOPK int = 1
const TOPK_SIZE int = 100

const INTERVAL int = 1000 // ms
const MILLION int = 1000000
const BILLION int = 1000000000


func AbsInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func AbsInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func AbsFloat64(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func SignInt(x int) int {
	if x < 0 {
		return -1
	} else {
		return 1
	}
}

func SignFloat64(x float64) int {
	if x < 0 {
		return -1
	} else {
		return 1
	}
}

func MinInt(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func i64tob(val uint64) []byte {
	r := make([]byte, 8)
	for i := uint64(0); i < 8; i++ {
		r[i] = byte((val >> (i * 8)) & 0xff)
	}
	return r
}

func btoi64(val []byte) uint64 {
	r := uint64(0)
	for i := uint64(0); i < 8; i++ {
		r |= uint64(val[i]) << (8 * i)
	}
	return r
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

func btoi32(val []byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(val[i]) << (8 * i)
	}
	return r
}