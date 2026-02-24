package types

type SkipListValue struct {
	Id          string //原始业务ID
	BitsFeature uint64 //64位特征码
	Score       float32
}
