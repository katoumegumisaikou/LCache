package lcache

type ByteView struct {
	b []byte
}

func (b *ByteView) Len() int {
	return len(b.b)
}

func (b *ByteView) ByteSlice() []byte {
	c := make([]byte, len(b.b))
	copy(c, b.b)
	return c
}

func (b *ByteView) String() string {
	return string(b.b)
}
