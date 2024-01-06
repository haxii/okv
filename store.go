package oss

import (
	"io"
	"net/url"
	"time"
)

type Store interface {
	Set(key string, reader io.Reader) error
	Get(key string) (io.ReadCloser, error)
	Del(key []string) error
	IsNotExistErr(error) bool
	PresignURL(key, method string, expired time.Duration) (*url.URL, error)
}
