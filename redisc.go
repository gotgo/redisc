package redisc

import (
	"math/rand"

	"github.com/amattn/deeperror"
	"github.com/garyburd/redigo/redis"
)

type Client interface {
	Write(command string, args ...interface{}) (interface{}, error)
	Read(command string, args ...interface{}) (interface{}, error)
	ReadInt64(command string, args ...interface{}) (int64, error)
}

type ScoredMember struct {
	Score  int
	Member string
}

func StringsToInterfaces(keys []string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		result[i] = k
	}
	return result
}

func GetMembers(members []*ScoredMember) []string {
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = m.Member
	}
	return keys
}

func Prefix(namespace string, keys []string) []string {
	result := make([]string, len(keys))
	for i, k := range keys {
		result[i] = namespace + k
	}
	return result
}

type KeyValueString struct {
	Key   string
	Value string
}

func flatten(kvs []*KeyValueString) []interface{} {
	r := make([]interface{}, 2*len(kvs))
	for i, kv := range kvs {
		j := i * 2
		r[j] = kv.Key
		r[j+1] = kv.Value
	}
	return r
}

func ArrayOfBytes(results interface{}, err error) ([][]byte, error) {
	if values, err := redis.Values(results, err); err != nil {
		return nil, err
	} else {
		result := make([][]byte, len(values))
		for i := 0; i < len(values); i++ {
			result[i] = values[i].([]byte)
		}
		return result, nil
	}
}

func ArrayOfStrings(results interface{}, err error) ([]string, error) {
	if values, err := redis.Values(results, err); err != nil {
		return nil, deeperror.New(rand.Int63(), "redis values fail", err)
	} else {
		result := make([]string, len(values))
		for i, value := range values {
			result[i], _ = redis.String(value, nil)
		}
		return result, nil
	}
}

func ArrayOfInts(results interface{}, err error) ([]int, error) {
	if values, err := redis.Values(results, err); err != nil {
		return nil, deeperror.New(rand.Int63(), "redis values fail", err)
	} else {
		result := make([]int, len(values))
		for i, value := range values {
			result[i], _ = redis.Int(value, nil)
		}
		return result, nil
	}
}
func join(key string, items []string) []interface{} {
	result := make([]interface{}, len(items)+1)
	result[0] = key
	for i, item := range items {
		result[i+1] = item
	}
	return result
}
