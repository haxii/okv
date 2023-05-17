package oss

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type FileStore struct {
	baseDir string
}

func NewFileStore(baseDir string) *FileStore {
	return &FileStore{baseDir: baseDir}
}

func (f *FileStore) IsNotExistErr(err error) bool {
	return os.IsNotExist(err)
}

func (f *FileStore) Set(key string, reader io.Reader) error {
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	p := filepath.Join(f.baseDir, key)
	if err = os.MkdirAll(filepath.Dir(p), 0777); err != nil {
		return err
	}
	return ioutil.WriteFile(p, bytes, 0777)
}

func (f *FileStore) Get(key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(f.baseDir, key))
}

func (f *FileStore) Del(keys []string) error {
	for _, key := range keys {
		if err := os.Remove(filepath.Join(f.baseDir, key)); err != nil {
			if f.IsNotExistErr(err) {
				return nil
			}
			return err
		}
	}
	return nil
}
