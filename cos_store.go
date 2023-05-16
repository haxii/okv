package okv

import (
	"context"
	"fmt"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type COSStore struct {
	client *cos.Client
}

func NewCOSStore(bucket, region string, secretID, secretKey string) *COSStore {
	_url, _ := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucket, region))
	baseURL := &cos.BaseURL{BucketURL: _url}
	client := cos.NewClient(baseURL, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})
	return &COSStore{client: client}
}

func (c *COSStore) Set(key string, reader io.Reader) error {
	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			CacheControl: "max-age=31536000",
		}}
	_, err := c.client.Object.Put(context.Background(), key, reader, opt)
	return err
}

func (c *COSStore) Get(key string) (io.ReadCloser, error) {
	resp, err := c.client.Object.Get(context.Background(), key, nil)
	return resp.Body, err
}

func (c *COSStore) Del(key []string) error {
	objs := make([]cos.Object, len(key))
	for i, k := range key {
		objs[i].Key = k
	}
	_, _, err := c.client.Object.DeleteMulti(context.Background(), &cos.ObjectDeleteMultiOptions{Objects: objs})
	return err
}

func (c *COSStore) IsNotExistErr(err error) bool {
	return strings.Contains(err.Error(), "404 NoSuchKey")
}
