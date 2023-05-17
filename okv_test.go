package oss

import (
	"fmt"
	"os"
	"testing"
)

func TestOKV(t *testing.T) {
	if err := os.MkdirAll("/tmp/okv", 0777); err != nil {
		t.Fatal(err)
	}
	store := NewFileStore("/tmp/okv")
	okv := NewOKV(store, Config{
		NameSpace:   "default",
		FileName:    "file",
		FileType:    "bin",
		GzCompress:  true,
		WriteThread: 10,
		ReadThread:  20,
	})
	if err := okv.PutOne("one_test", []byte("The quick brown fox jumps over the lazy dog")); err != nil {
		t.Fatal(err)
	}
	got, err := okv.GetOne("one_test")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(got))
	if err = okv.Del([]string{"one_test"}); err != nil {
		t.Fatal(err)
	}

	many := make(map[string][]byte)
	keys := make([]string, 0)
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Sprintf("many_%03d", i))
		many[fmt.Sprintf("many_%03d", i)] = []byte(fmt.Sprintf(
			"%03d: The quick brown fox jumps over the lazy dog", i))
	}
	if err = okv.Put(many); err != nil {
		t.Fatal(err)
	}
	gotMany, err := okv.Get(keys)
	if err != nil {
		t.Fatal(err)
	}
	for key, bytes := range gotMany {
		t.Log(key, string(bytes))
	}
	if err = okv.Del(keys); err != nil {
		t.Fatal(err)
	}

	// again should be nil
	gotMany, err = okv.Get(keys)
	if err != nil {
		t.Fatal(err)
	}
	for key, bytes := range gotMany {
		t.Log(key, string(bytes))
	}

}
