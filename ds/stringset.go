package ds

type StringSet struct {
	m map[string]struct{}
}

func NewStringSet(initial []string) *StringSet {
	s := &StringSet{
		m: make(map[string]struct{}),
	}
	for _, e := range initial {
		s.m[e] = struct{}{}
	}
	return s
}

func (s *StringSet) Difference(o *StringSet) (difference []string) {
	for k := range s.m {
		if _, ok := o.m[k]; !ok {
			difference = append(difference, k)
		}
	}
	return
}

func (s *StringSet) ToSlice() (slice []string) {
	for val := range s.m {
		slice = append(slice, val)
	}
	return
}
