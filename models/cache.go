package model

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"time"
)

type Cache struct {
	gorm.Model
	KeyName   string `gorm:"unique_index:idx_only_one_key_name"`
	Value     string `gorm:"type:varbinary(2048)"`
	ExpiredAt time.Time
}

func DeleteCacheByKey(key string) error {
	result := DB.Unscoped().Where("key_name = ?", key).Delete(&Cache{})
	return result.Error
}

func DeleteAllCache() error {
	result := DB.Unscoped().Where("1 = 1").Delete(&Cache{})
	return result.Error
}

func GetCacheByKey(key string) (Cache, error) {
	var m Cache
	result := DB.Where("key_name = ?", key).First(&m)
	if result.Error != nil {
		return m, result.Error
	}
	if time.Now().After(m.ExpiredAt) {
		err := DeleteCacheByKey(key)
		if err != nil {
			return m, err
		}
		return m, errors.New("expired cache")
	}
	return m, nil
}

func SaveCache(cache Cache) error {
	DeleteCacheByKey(cache.KeyName)
	return DB.Save(&cache).Error
}

type item struct {
	Value interface{}
}

func serializer(value interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	storeValue := item{
		Value: value,
	}
	err := enc.Encode(storeValue)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func deserializer(value []byte) (interface{}, error) {
	var res item
	buffer := bytes.NewReader(value)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&res)
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

var cacheStore = NewDBCacheStore()

func NewDBCacheStore() *DBCacheStore {
	fmt.Println("NewDBCacheStore")
	return &DBCacheStore{}
}

type DBCacheStore struct{}

// Set 存储值
func (store *DBCacheStore) Set(key string, value interface{}, ttl int) error {
	serialized, err := serializer(value)
	if err != nil {
		return err
	}

	m := Cache{
		KeyName: key,
		Value:   string(serialized),
	}
	if ttl > 0 {
		m.ExpiredAt = time.Now().Add(time.Duration(ttl) * time.Second)
	} else {
		m.ExpiredAt = time.Now().Add(1 * time.Hour)
	}
	err = SaveCache(m)
	if err != nil {
		fmt.Printf("DBCacheStore Set: %s, %v \n", key, err)
	}
	return err
}

// Get 取值
func (store *DBCacheStore) Get(key string) (interface{}, bool) {
	m, err := GetCacheByKey(key)
	if err != nil && err.Error() != "record not found" {
		fmt.Printf("DBCacheStore Get: %s, %v \n", key, err)
	}
	if err != nil {
		return nil, false
	}

	finalValue, err := deserializer([]byte(m.Value))
	if err != nil {
		return nil, false
	}

	return finalValue, true

}

// Gets 批量取值
func (store *DBCacheStore) Gets(keys []string, prefix string) (map[string]interface{}, []string) {
	var res = map[string]interface{}{}
	var missed = make([]string, 0, len(keys))
	for _, key := range keys {
		val, has := store.Get(prefix + key)
		if !has {
			missed = append(missed, key)
		} else {
			res[key] = val
		}
	}
	// 解码所得值
	return res, missed
}

// Sets 批量设置值
func (store *DBCacheStore) Sets(values map[string]interface{}, prefix string) error {
	for key, value := range values {
		err := store.Set(key, value, 0)
		if err != nil {
			return err
		}
	}
	return nil

}

// Delete 批量删除给定的键
func (store *DBCacheStore) Delete(keys []string, prefix string) error {
	for _, key := range keys {
		err := DeleteCacheByKey(prefix + key)
		fmt.Printf("DBCacheStore Delete: %+v, %v \n", keys, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *DBCacheStore) Deletes(keys []string, prefix string) error {
	return store.Delete(keys, prefix)
}

// DeleteAll 批量所有键
func (store *DBCacheStore) DeleteAll() error {
	err := DeleteAllCache()
	return err
}

// Persist Dummy implementation
func (store *DBCacheStore) Persist(path string) error {
	return nil
}

// Restore dummy implementation
func (store *DBCacheStore) Restore(path string) error {
	return nil
}

// GetSettings 根据名称批量获取设置项缓存
func (store *DBCacheStore) GetSettings(keys []string, prefix string) (map[string]string, []string) {
	raw, miss := store.Gets(keys, prefix)

	res := make(map[string]string, len(raw))
	for k, v := range raw {
		res[k] = v.(string)
	}

	return res, miss
}

// SetSettings 批量设置站点设置缓存
func (store *DBCacheStore) SetSettings(values map[string]string, prefix string) error {
	var toBeSet = make(map[string]interface{}, len(values))
	for key, value := range values {
		toBeSet[key] = interface{}(value)
	}
	return store.Sets(toBeSet, prefix)
}
