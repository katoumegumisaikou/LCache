package lcache

import (
	"testing"
)

func TestByteView_Len(t *testing.T) {
	bv := ByteView{b: []byte("hello")}
	if bv.Len() != 5 {
		t.Fatalf("expected len 5, got %d", bv.Len())
	}

	bv2 := ByteView{b: []byte{}}
	if bv2.Len() != 0 {
		t.Fatalf("expected len 0, got %d", bv2.Len())
	}
}

func TestByteView_ByteSlice(t *testing.T) {
	original := []byte("hello world")
	bv := ByteView{b: cloneBytes(original)}

	// ByteSlice 返回的是副本，修改不应影响原值
	slice := bv.ByteSlice()
	slice[0] = 'H'

	if bv.String() != "hello world" {
		t.Fatal("ByteSlice should return a copy, not a reference")
	}
}

func TestByteView_String(t *testing.T) {
	bv := ByteView{b: []byte("hello")}
	if bv.String() != "hello" {
		t.Fatalf("expected 'hello', got %s", bv.String())
	}
}

func TestCloneBytes(t *testing.T) {
	original := []byte("test-data")
	cloned := cloneBytes(original)

	// 修改原值不应影响克隆值
	original[0] = 'X'
	if string(cloned) != "test-data" {
		t.Fatal("cloneBytes should return a copy")
	}
}
