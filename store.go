package okv

import "io"

type Store interface {
	Set(key string, reader io.Reader) error
	Get(key string) (io.ReadCloser, error)
	Del(key []string) error
	IsNotExistErr(error) bool
}
