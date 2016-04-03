package cache

// Need to realize Len(), Less(), Swap(), Push() and Pop()
type heapInterface interface {
	Len() int
	Less(i, j int) bool
	Swap(i, j int)
	Push(x interface{})
	Pop() interface{}
}

func up(h heapInterface, j int) {
	for {
		parent := (j-1) / 2
		if parent == j || !h.Less(j,parent) {
			break
		}
		h.Swap(j, parent)
		j = parent
	}
}

func down(h heapInterface, i, len int) {
	for {
		left := i * 2 + 1
		if left >= len || left < 0 {
			break
		}
		right := left + 1
		minone := left
		if right < len && !h.Less(left,right) {
			minone = right
		}
		if !h.Less(minone, i) {
			break
		}
		h.Swap(i, minone)
		i = minone
	}
}

func Push(h heapInterface, x interface{}) {
	h.Push(x)
	up(h, h.Len()-1)
}

func Pop(h heapInterface) interface{} {
	len := h.Len() - 1
	h.Swap(0, len)
	down(h, 0 ,len)
	return h.Pop()
}

func Init(h heapInterface) {
	len := h.Len()
	for i := len / 2 - 1; i >= 0; i-- {
		down(h,i,len)
	}
}

func Remove(h heapInterface, i int) interface{} {
	len := h.Len() - 1
	if len != i {
		h.Swap(i, len)
		down(h, i, len)
		up(h, i)
	}
	return h.Pop()
}

func FixValue(h heapInterface, i int) {
	down(h, i ,h.Len())
	up(h, i)
}