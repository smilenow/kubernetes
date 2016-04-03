package cache

import (
	"k8s.io/kubernetes/pkg/api"
	"testing"
	"reflect"
	"time"
)

func testPQObjectKeyFunc(obj interface{}) (string, error) {
	return obj.(*api.Pod).Spec.NodeName, nil
}

func mktestPQObj(name string, prioirty *int64) *api.Pod {
	podSpec := api.PodSpec{
		NodeName: name,
		Priority: prioirty,
	}
	return &api.Pod{
		Spec: podSpec,
	}
}

func TestPQ_addUpdate(t *testing.T) {
	f := NewPQ(testPQObjectKeyFunc)
	foo1 := new(int64)
	*foo1 = int64(1)
	foo2 := new(int64)
	*foo2 = int64(2)
	f.Add(mktestPQObj("foo", foo1))
	f.Update(mktestPQObj("foo", foo2))

	if e, a := []interface{}{mktestPQObj("foo", foo2)}, f.List(); !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %+v, got %+v", e, a)
	}

	if e,a := []string{"foo"}, f.ListKeys(); !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %+v, got %+v", e, a)
	}

	got := make(chan *api.Pod, 2)
	go func() {
		for {
			got <- f.Pop().(*api.Pod)
		}
	}()

	first := <-got
	if e,a := int64(2), first.Spec.Priority; e != *a {
		t.Errorf("Didn't get updated value (%v), got %v", e, *a)
	}
	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", *unexpected.Spec.Priority)
	case <-time.After(50 * time.Millisecond):
	}
	_, exists, _ := f.Get(mktestPQObj("foo", nil))
	if exists {
		t.Errorf("item did not get removed")
	}
}

func TestPQ_addReplace(t *testing.T) {
	f := NewPQ(testPQObjectKeyFunc)
	foo1 := new(int64)
	*foo1 = int64(1)
	foo2 := new(int64)
	*foo2 = int64(2)
	f.Add(mktestPQObj("foo", foo1))
	f.Replace([]interface{}{mktestPQObj("foo", foo2)}, "2")

	got := make(chan *api.Pod, 2)
	go func() {
		for {
			got <- f.Pop().(*api.Pod)
		}
	}()

	first := <-got
	if e,a := int64(2), first.Spec.Priority; e != *a {
		t.Errorf("Didn't get updated value (%v), got %v", e, *a)
	}

	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", *unexpected.Spec.Priority)
	case <-time.After(50 * time.Millisecond):
	}
	_, exists, _ := f.Get(mktestPQObj("foo", nil))
	if exists {
		t.Errorf("item did not get removed")
	}
}

func TestPQ_detectLineJumpers(t *testing.T) {
	f := NewPQ(testPQObjectKeyFunc)

	foo10, bar1, foo11, foo13, zab30 := new(int64), new(int64), new(int64), new(int64), new(int64)
	*foo10, *bar1, *foo11, *foo13, *zab30 = int64(10), int64(1), int64(11), int64(13), int64(30)

	f.Add(mktestPQObj("foo", foo10))
	f.Add(mktestPQObj("bar", bar1))
	f.Add(mktestPQObj("foo", foo11))
	f.Add(mktestPQObj("foo", foo13))
	f.Add(mktestPQObj("zab", zab30))

	// zab pop according to its priority is maximum
	if e, a := int64(30), f.Pop().(*api.Pod).Spec.Priority; e != *a {
		t.Fatalf("expected %d, got %d", e, *a)
	}

	zab5 := new(int64)
	*zab5 = int64(5)
	f.Add(mktestPQObj("zab", zab5)) // To ensure zab doesn't jump back in line

	// pop foo
	if e, a := int64(13), f.Pop().(*api.Pod).Spec.Priority; e != *a {
		t.Fatalf("expected %d, got %d", e, *a)
	}

	// pop zab
	if e, a := int64(5), f.Pop().(*api.Pod).Spec.Priority; e != *a {
		t.Fatalf("expected %d, got %d", e, *a)
	}

	// pop bar
	if e, a := int64(1), f.Pop().(*api.Pod).Spec.Priority; e != *a {
		t.Fatalf("expected %d, got %d", e, *a)
	}
}

func TestPQ_addIfNotPresent(t *testing.T) {
	f := NewPQ(testPQObjectKeyFunc)

	v1, v2, v3, v4 := new(int64), new(int64), new(int64), new(int64)
	*v1, *v2, *v3, *v4 = int64(1), int64(2), int64(3), int64(4)
	f.Add(mktestPQObj("a", v1))
	f.Add(mktestPQObj("b", v2))
	f.AddIfNotPresent(mktestPQObj("b", v3))
	f.AddIfNotPresent(mktestPQObj("c", v4))

	if e, a := 3, len(f.items); a != e {
		t.Fatalf("expected queue length %d, got %d", e, a)
	}

	expectedValues := []int64{4, 2, 1}
	for _, expected := range expectedValues {
		if actual := f.Pop().(*api.Pod).Spec.Priority; expected != *actual {
			t.Fatalf("expected value %d, got %d", expected, *actual)
		}
	}
}

func TestPQ_HasSynced(t *testing.T) {
	tests := []struct {
		actions		[]func(f *PQ)
		expectedSynced	bool
	}{
		{
			actions:	[]func(f *PQ){},
			expectedSynced:	false,
		},
		{
			actions:	[]func(f *PQ){
				func (f *PQ) {
					v1 := new(int64)
					*v1 = int64(1)
					f.Add(mktestPQObj("a", v1))
				},
			},
			expectedSynced:	true,
		},
		{
			actions:	[]func(f *PQ){
				func(f *PQ) { f.Replace([]interface{}{}, "0") },
			},
			expectedSynced: true,
		},
		{
			actions:	[]func(f *PQ){
				func(f *PQ) {
					v1, v2 := new(int64), new(int64)
					*v1, *v2 = int64(1), int64(2)
					f.Replace([]interface{}{mktestPQObj("a", v1), mktestPQObj("b", v2)}, "0")
				},
			},
			expectedSynced: false,
		},
		{
			actions:	[]func(f *PQ){
				func(f *PQ) {
					v1, v2 := new(int64), new(int64)
					*v1, *v2 = int64(1), int64(2)
					f.Replace([]interface{}{mktestPQObj("a", v1), mktestPQObj("b", v2)}, "0")
				},
				func(f *PQ) { f.Pop() },
			},
			expectedSynced: false,
		},
		{
			actions:	[]func(f *PQ){
				func(f *PQ) {
					v1, v2 := new(int64), new(int64)
					*v1, *v2 = int64(1), int64(2)
					f.Replace([]interface{}{mktestPQObj("a", v1), mktestPQObj("b", v2)}, "0")
				},
				func(f *PQ) { f.Pop() },
				func(f *PQ) { f.Pop() },
			},
			expectedSynced: true,
		},
	}

	for i, test := range tests {
		f := NewPQ(testPQObjectKeyFunc)

		for _, action := range test.actions {
			action(f)
		}
		if e, a := test.expectedSynced, f.HasSynced(); a != e {
			t.Errorf("test case %v failed, expected: %v , got %v", i, e, a)
		}
	}
}