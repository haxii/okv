package oss

import (
	"bytes"
	"compress/gzip"
	"github.com/haxii/task"
	"io"
	"io/ioutil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KV struct {
	Key string
	Val []byte
}

type OKV struct {
	conf  Config
	store Store
}

func NewOKV(store Store, conf Config) *OKV {
	return &OKV{store: store, conf: conf}
}

func (o *OKV) Put(data map[string][]byte) error {
	kvList := make([]*KV, len(data))
	i := 0
	for k, v := range data {
		kvList[i] = &KV{Key: k, Val: v}
		i++
	}
	return o.PutBatch(kvList)
}

func (o *OKV) Get(keys []string) (map[string][]byte, error) {
	kvList, err := o.GetBatch(keys)
	kvMap := make(map[string][]byte, len(kvList))
	for _, kvData := range kvList {
		kvMap[kvData.Key] = kvData.Val
	}
	return kvMap, err
}

func (o *OKV) PutBatch(kvList []*KV) error {
	if len(kvList) == 0 {
		return nil
	}
	if len(kvList) == 1 {
		return o.PutOne(kvList[0].Key, kvList[0].Val)
	}
	indexList := makeIndexList(len(kvList))
	return task.Execute(indexList, o.conf.WriteThread, func(indexStr string) error {
		index, _ := strconv.Atoi(indexStr)
		kv := kvList[index]
		return o.PutOne(kv.Key, kv.Val)
	})
}

func (o *OKV) GetBatch(keys []string) ([]*KV, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) == 1 {
		k := keys[0]
		v, err := o.GetOne(k)
		if err != nil {
			return nil, err
		}
		return []*KV{{Key: k, Val: v}}, nil
	}

	indexList := makeIndexList(len(keys))
	kvList := make([]*KV, len(keys))
	var kvListMu sync.Mutex
	err := task.Execute(indexList, o.conf.ReadThread, func(indexStr string) error {
		index, _ := strconv.Atoi(indexStr)
		key := keys[index]
		val, err := o.GetOne(key)
		if err != nil {
			return err
		}
		kvListMu.Lock()
		defer kvListMu.Unlock()
		kvList[index] = &KV{Key: key, Val: val}
		return nil
	})
	return kvList, err
}

func (o *OKV) Del(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	pathList := make([]string, len(keys))
	for i, key := range keys {
		pathList[i] = o.Path(key)
	}
	return o.store.Del(pathList)
}

func (o *OKV) PutOne(key string, val []byte) error {
	if !o.conf.GzCompress {
		return o.store.Set(o.Path(key), bytes.NewReader(val))
	}
	pr, pw := io.Pipe()
	defer func() {
		_ = pr.Close()
	}()

	setErrChan := make(chan error)
	go func() {
		setErrChan <- o.store.Set(o.Path(key), pr)
		close(setErrChan)
	}()

	gzpw := gzip.NewWriter(pw)
	_, err := gzpw.Write(val)
	_ = gzpw.Close()
	_ = pw.Close()
	if saveErr := <-setErrChan; saveErr != nil {
		return saveErr
	}
	return err
}

func (o *OKV) GetOne(key string) ([]byte, error) {
	r, err := o.store.Get(o.Path(key))
	if err != nil {
		if o.store.IsNotExistErr(err) {
			return nil, nil
		}
		return nil, err
	}
	defer r.Close()
	if !o.conf.GzCompress {
		return ioutil.ReadAll(r)
	}
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(gr)
}

// Path returns the object path of key, be likes
// namespace/key[/filename][.ext][.gz]
func (o *OKV) Path(key string) string {
	p := path.Join(o.conf.NameSpace, key, o.conf.FileName)
	var b strings.Builder
	b.WriteString(p)
	if ext := o.conf.FileType; len(ext) > 0 {
		if !strings.HasPrefix(ext, ".") {
			b.WriteByte('.')
		}
		b.WriteString(ext)
	}
	if o.conf.GzCompress {
		b.WriteString(".gz")
	}
	return b.String()
}

// PresignURL 上传(put)/下载(get)
func (o *OKV) PresignURL(key string, method string, expired time.Duration) (*url.URL, error) {
	return o.store.PresignURL(o.Path(key), method, expired)
}

func makeIndexList(len int) []string {
	if len <= 0 {
		return nil
	}
	l := make([]string, len)
	for i := 0; i < len; i++ {
		l[i] = strconv.Itoa(i)
	}
	return l
}
