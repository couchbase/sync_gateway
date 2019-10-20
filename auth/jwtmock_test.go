package auth

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeToStringBase64(t *testing.T) {
	message := "At my signal, unleash hell!"
	actual := EncodeToStringBase64(message)
	expected := "QXQgbXkgc2lnbmFsLCB1bmxlYXNoIGhlbGwh"
	assert.Equal(t, expected, actual)
}

func TestGetHMACSHA256(t *testing.T) {
	message := "Chewie, we're home."
	actual := GetHMACSHA256(message, "secret")
	expected := "eElwiF_hBhT_H3Dm7X96IOGMEFjzDVjoJzwunrP4dWA"
	assert.Equal(t, expected, actual)
}

func TestGetStandardHeader(t *testing.T) {
	header := make(map[string]interface{})
	header["alg"] = "HS256"
	header["typ"] = "JWT"
	actual := GetStandardHeader()
	assert.True(t, reflect.DeepEqual(header, actual))
}

func TestToJson(t *testing.T) {
	header := make(map[string]interface{})
	header["alg"] = "HS256"
	header["typ"] = "JWT"

	jsonHeader, err := toJson(header)
	expected := "{\"alg\":\"HS256\",\"typ\":\"JWT\"}"
	assert.NoError(t, err)
	assert.Equal(t, expected, jsonHeader)
}

func TestToJsonWithEmptyMap(t *testing.T) {
	header := make(map[string]interface{})
	jsonHeader, err := toJson(header)
	assert.NoError(t, err)
	assert.Equal(t, "{}", jsonHeader)
}

func TestToJsonWithNil(t *testing.T) {
	jsonHeader, err := toJson(nil)
	assert.NoError(t, err)
	assert.Equal(t, "null", jsonHeader)
}

func TestToJsonWithUnSupportedType(t *testing.T) {
	header := make(map[string]interface{})
	header["alg"] = "HS256"
	header["typ"] = func() {}

	jsonHeader, err := toJson(header)
	assert.Error(t, err)
	assert.Empty(t, jsonHeader)
}

func TestGetBearerToken(t *testing.T) {
	// Mock up a payload or claim for token
	claims := func() map[string]interface{} {
		audience := [...]string{"ebay", "comcast", "linkedin"}
		claims := make(map[string]interface{})
		claims["id"] = "CB00912"
		claims["iss"] = "https://accounts.google.com"
		claims["sub"] = "1234567890"
		claims["name"] = "John Wick"
		claims["aud"] = audience
		claims["iat"] = 1516239022
		claims["exp"] = 1586239022
		claims["email"] = "johnwick@couchbase.com"
		return claims
	}
	header := GetStandardHeaderAsJSON()
	payload, err := toJson(claims())
	assert.NoError(t, err)
	token := GetBearerToken(header, payload, "secret")
	assert.Equal(t, uint32(0x45c5582a), hash(token))
}

func TestGetStandardHeaderAsJSON(t *testing.T) {
	header := GetStandardHeaderAsJSON()
	expected := "{\"alg\":\"HS256\",\"typ\":\"JWT\"}"
	assert.Equal(t, expected, header)
}

func TestHash(t *testing.T) {
	assert.Equal(t, uint32(0xcc953b7f), hash("Maximus"))
	assert.Equal(t, uint32(0x811c9dc5), hash(""))
	assert.Equal(t, uint32(0xf9808ff2), hash("0123456789"))
	assert.Equal(t, uint32(0x2c46220), hash("Mǎkè xī mǔ sī"))
}
