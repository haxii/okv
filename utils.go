package okv

import (
	"encoding/hex"
	"github.com/google/uuid"
	"path/filepath"
	"strings"
	"time"
)

var ShanghaiLoc, _ = time.LoadLocation("Asia/Shanghai")

// makePath 给定文件名，仅保留后缀生成一个形式为  Year/Month/Day/xxxxxxxx/xxxx/xxxx/xxxx/xxxxxxxxxxxx.ext 的文件路径
func makePath(fileName string) string {
	return datePath() + "/" + randomUUIDPath() + strings.ToLower(filepath.Ext(fileName))
}

func datePath() string {
	return time.Now().In(ShanghaiLoc).Format("2006/01/02")
}

// randomUUIDPath 生成一个形式为 xxxxxxxx/xxxx/xxxx/xxxx/xxxxxxxxxxxx 的文件路径
func randomUUIDPath() string {
	var buf [36]byte
	encodeUUIDPath(buf[:], uuid.New())
	return string(buf[:])
}

func encodeUUIDPath(dst []byte, uuid uuid.UUID) {
	hex.Encode(dst, uuid[:4])
	dst[8] = '/'
	hex.Encode(dst[9:13], uuid[4:6])
	dst[13] = '/'
	hex.Encode(dst[14:18], uuid[6:8])
	dst[18] = '/'
	hex.Encode(dst[19:23], uuid[8:10])
	dst[23] = '/'
	hex.Encode(dst[24:], uuid[10:])
}
