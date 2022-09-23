package main

import (
	"fmt"
	"net/url"
	"reflect"
)

func walkInner(val reflect.Value, fullKey string, walker func(key, fullKey string, m map[string]any) bool) error {
	switch val.Kind() {
	case reflect.Map:
		if val.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("walkInner %s: invalid key type %v", fullKey, val.Type().Key())
		}
		for _, k := range val.MapKeys() {
			newFullKey := fmt.Sprintf("%s.%s", fullKey, k.String())
			if newFullKey[0] == '.' {
				newFullKey = newFullKey[1:]
			}
			if walker(k.String(), newFullKey, val.Interface().(map[string]any)) {
				mv := val.MapIndex(k)
				if mv.Kind() == reflect.Interface || mv.Kind() == reflect.Ptr {
					mv = mv.Elem()
				}
				if mv.Kind() != reflect.Map {
					continue
				}
				err := walkInner(mv, newFullKey, walker)
				if err != nil {
					return err
				}
			}
		}
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			v := val.Index(i)
			if v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			if v.Kind() != reflect.Map && v.Kind() != reflect.Slice {
				continue
			}
			err := walkInner(v, fmt.Sprintf("%s[%d]", fullKey, i), walker)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("walkInner %s: invalid type %v", fullKey, val.Type())
	}
	return nil
}

// walkJSON walks a decoded JSON object from json.Unmarshal (called on a map[string]any value).
// It calls the given walker on every nested object field. walkJSON steps through arrays, i.e., if the arrays themselves
// contain objects it will call walker on them, but it will not call walker on other array members.
// The walker will be called with the current value's key, the full dot-separated path to it, and the map itself.
// Any changes to the map by the walker other than modifying a scalar value (such as inserting or removing elements)
// may or may not be visible in subsequent calls to the walker.
// The walker can return false to avoid recursing further into the current object.
func walkJSON(m any, walker func(key, fullKey string, m map[string]any) bool) error {
	return walkInner(reflect.ValueOf(m), "", walker)
}

const maskedPassword = "*****"

func RemovePasswordsAndTagUserData(val map[string]any) error {
	return walkJSON(val, func(key, fullKey string, m map[string]any) bool {
		switch key {
		case "password":
			m[key] = maskedPassword
			return false
		case "server":
			if sv, ok := m[key].(string); ok {
				m[key] = stripPasswordFromURL(sv)
			}
			return false
		case "username":
			if sv, ok := m[key].(string); ok {
				m[key] = UD(sv)
			}
			return false
		case "users", "roles":
			// We need to modify map keys, so the usual recursion behaviour isn't sufficient
			m = m[key].(map[string]any)
			for k, v := range m {
				if len(k) > 4 && k[:4] == "<ud>" {
					continue
				}
				userVal := v
				m[UD(k)] = userVal
				delete(m, k)
			}
			for _, v := range m {
				userInfo, ok := v.(map[string]any)
				if !ok {
					return false
				}
				if name, ok := userInfo["name"].(string); ok {
					userInfo["name"] = UD(name)
				}
				if _, ok := userInfo["password"]; ok {
					userInfo["password"] = maskedPassword
				}
				if chans, ok := userInfo["admin_channels"].([]any); ok {
					for i, ch := range chans {
						if strVal, ok := ch.(string); ok {
							chans[i] = UD(strVal)
						}
					}
				}
				if roles, ok := userInfo["admin_roles"].([]any); ok {
					for i, role := range roles {
						if strVal, ok := role.(string); ok {
							roles[i] = UD(strVal)
						}
					}
				}
			}
			return false
		default:
			return true
		}
	})
}

func stripPasswordFromURL(urlStr string) string {
	urlVal, err := url.Parse(urlStr)
	if err != nil {
		return urlStr
	}
	if _, ok := urlVal.User.Password(); !ok {
		return urlStr
	}
	return fmt.Sprintf(`%s://%s:%s@%s%s`,
		urlVal.Scheme, urlVal.User.Username(), maskedPassword, urlVal.Host, urlVal.Path)
}

func UD(s string) string {
	return "<ud>" + s + "</ud>"
}
