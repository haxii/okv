package okv

import (
	"context"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
)

type COSStore struct {
	client *cos.Client
}

func NewCOSStore(client *cos.Client) *COSStore {
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
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
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
	return cos.IsNotFoundError(err)
}
