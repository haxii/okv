package okv

import "io"

type Store interface {
	Set(key string, reader io.Reader) error
	Get(Key string) (io.ReadCloser, error)
	Del(Key []string) error
}
