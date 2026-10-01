package distributedjob

import "testing"

// La grammatica di `sort` deve significare la stessa cosa sui due backend: era parsata due volte,
// una per query store, con due cicli scritti a mano.
func TestParseSort(t *testing.T) {
	casi := map[string][]SortField{
		"":                    nil,
		"   ":                 nil,
		"createTime":          {{Column: "createTime"}},
		"createTime:desc":     {{Column: "createTime", Desc: true}},
		"createTime:DESC":     {{Column: "createTime", Desc: true}},
		" a , b:desc ,c:asc ": {{Column: "a"}, {Column: "b", Desc: true}, {Column: "c"}},
		"a,,b":                {{Column: "a"}, {Column: "b"}},
		"a:qualsiasicosa":     {{Column: "a"}},
	}
	for in, atteso := range casi {
		got := ParseSort(in)
		if len(got) != len(atteso) {
			t.Errorf("ParseSort(%q) = %+v, atteso %+v", in, got, atteso)
			continue
		}
		for i := range got {
			if got[i] != atteso[i] {
				t.Errorf("ParseSort(%q)[%d] = %+v, atteso %+v", in, i, got[i], atteso[i])
			}
		}
	}
}
