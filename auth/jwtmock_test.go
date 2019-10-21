package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
)

func encodeToStringBase64(key string) string {
	value := base64.URLEncoding.EncodeToString([]byte(key))
	return strings.TrimRight(value, "=")
}

func getHMACSHA256(data, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(data))
	hash := base64.URLEncoding.EncodeToString(h.Sum(nil))
	return strings.TrimRight(hash, "=")
}

func getBearerToken(header, payload, secret string) string {
	h := encodeToStringBase64(header)
	p := encodeToStringBase64(payload)
	hp := fmt.Sprintf("%s.%s", h, p)
	s := getHMACSHA256(hp, secret)
	return fmt.Sprintf("%s.%s.%s", h, p, s)
}

func toJson(data map[string]interface{}) (string, error) {
	json, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(json), nil
}

func getStandardHeader() map[string]interface{} {
	header := make(map[string]interface{})
	header["alg"] = "HS256"
	header["typ"] = "JWT"
	return header
}

func getStandardHeaderAsJSON() string {
	header, _ := toJson(getStandardHeader())
	return header
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
