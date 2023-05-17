package okv

import (
	"archive/zip"
	"bytes"
	"io"
	"path"
)

type OSS struct {
	store      Store
	pathPrefix string
}
git
func NewOSS(store Store, pathPrefix string) *OSS {
	return &OSS{store: store, pathPrefix: pathPrefix}
}

// 给定一个 filePath 给其增加存储前缀
func (c *OSS) fullPath(filePath string) string {
	return path.Join(c.pathPrefix, filePath)
}

// Upload file to COS
func (c *OSS) Upload(filename string, reader io.Reader) (string, error) {
	relativePath := makePath(filename)
	err := c.store.Set(c.fullPath(relativePath), reader)
	if err != nil {
		return "", err
	}
	return relativePath, nil
}

// Delete file form COS
func (c *OSS) Delete(filePath []string) error {
	filePathList := make([]string, len(filePath))
	for i := range filePath {
		filePathList[i] = c.fullPath(filePath[i])
	}
	return c.store.Del(filePathList)
}

func (c *OSS) Get(filePath string) (io.ReadCloser, error) {
	return c.store.Get(c.fullPath(filePath))
}

func (c *OSS) getByte(filePath string) ([]byte, error) {
	resp, err := c.Get(filePath)
	if err != nil {
		return nil, err
	}
	defer resp.Close()
	return io.ReadAll(resp)
}

func (c *OSS) UnZip(filePath string, rename func(file *zip.File) string) (map[string][]byte, error) {
	obj, err := c.getByte(filePath)
	if err != nil {
		return nil, err
	}
	zipReader, err := zip.NewReader(bytes.NewReader(obj), int64(len(obj)))
	if err != nil {
		return nil, err
	}
	data := make(map[string][]byte)
	for _, file := range zipReader.File {
		name := rename(file)
		if len(name) == 0 {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, err
		}
		data[name] = b
	}
	return data, nil
}
