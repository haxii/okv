package oss

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
)

func newOSS() *OSS {
	if err := os.MkdirAll("/tmp/okv", 0777); err != nil {
		fmt.Println(err)
		return nil
	}
	store := NewFileStore("/tmp/okv")
	return NewOSS(store, "upload")
}

func TestOSS_Upload(t *testing.T) {
	oss := newOSS()
	url, err := oss.Upload("test", bytes.NewBufferString("test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(url)
	r, _ := oss.Get(url)
	b, err := io.ReadAll(r)
	t.Log(string(b), err)
}

func TestOSS_Delete(t *testing.T) {
	url := "2023/05/17/88d4fcd2/116d/4558/b4ca/d4fd3ce1cf57"
	oss := newOSS()
	r, err := oss.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(r)
	t.Log(string(b), err)
	_ = oss.Delete([]string{url})
	r, err = oss.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b), err)
}

func TestOSS_UnZip(t *testing.T) {
	oss := newOSS()
	files, err := oss.UnZip("2023/05/15/9e2ded3a/e6ae/4baf/ac33/b0e30dbc140e.zip", func(file *zip.File) string {
		if file.FileHeader.FileInfo().IsDir() {
			return ""
		}
		// 只处理 .xml 和 .pdf
		ext := path.Ext(file.Name)
		if ext == ".ofd" || ext == ".xlsx" {
			return ""
		}
		name := path.Base(file.Name)
		// 有打包隐藏文件的情况
		// __MACOSX/发票批量下载_20230510153744/._dzfp_2392200000000XXXXXX9_东莞市XXX有限公司_20230510153733.pdf
		if strings.HasPrefix(name, ".") {
			return ""
		}
		return name
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, _ := range files {
		t.Log(name)
	}
}
