package types

func NewTermQuery(field, word string) *TermQuery {
	return &TermQuery{
		Keyword: &Keyword{
			Field: field,
			Word:  word,
		},
	}
}

func (q *TermQuery) And(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}
	res := &TermQuery{
		Must: make([]*TermQuery, 0, len(queries)+1),
	}
	if q.Keyword != nil || len(q.Must) > 0 || len(q.Should) > 0 {
		res.Must = append(res.Must, q)
	}
	res.Must = append(res.Must, queries...)
	return res
}

func (q *TermQuery) Or(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}
	res := &TermQuery{
		Should: make([]*TermQuery, 0, len(queries)+1),
	}
	if q.Keyword != nil || len(q.Must) > 0 || len(q.Should) > 0 {
		res.Should = append(res.Should, q)
	}
	res.Should = append(res.Should, queries...)
	return res
}
