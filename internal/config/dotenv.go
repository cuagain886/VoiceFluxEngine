package config

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"strings"
)

// LoadDotEnv 读取 path 处的 .env（若存在），把其中的 KEY=VALUE 注入进程环境，
// 但**不覆盖已存在的环境变量**（真实环境优先，遵循 dotenv 惯例）。文件不存在
// 不算错误——返回 nil。有了它，`go run ./cmd/server` 无需先手动把 .env source
// 进会话。
//
// 解析规则（刻意保持极简）：忽略空行与 # 开头的注释；支持可选的 `export ` 前缀；
// 以第一个 = 切分键值；去掉值两端成对的引号。不处理行内注释（值里可能含 #）。
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue // 真实环境变量优先，绝不覆盖
		}
		if err := os.Setenv(key, unquote(strings.TrimSpace(val))); err != nil {
			return err
		}
	}
	return sc.Err()
}

// unquote 去掉值两端成对的单/双引号。
func unquote(s string) string {
	if len(s) >= 2 {
		if c := s[0]; (c == '"' || c == '\'') && s[len(s)-1] == c {
			return s[1 : len(s)-1]
		}
	}
	return s
}
