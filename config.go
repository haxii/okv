package okv

type Config struct {
	NameSpace   string
	FileName    string
	FileType    string
	GzCompress  bool // use gz compress
	WriteThread int
	ReadThread  int
}
