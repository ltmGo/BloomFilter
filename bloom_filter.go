package bulong_filter

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"math"
	"strconv"
)

//布隆过滤器

var redisClient *redis.Client

//把string转二级制
func stringToBin(s string) (binString string) {
	for _, c := range s {
		binString = fmt.Sprintf("%s%b ",binString, c)
	}
	return
}


func (b *bloomFilter) connectionRedis() *redis.Client {
	if redisClient == nil {
		redisClient = redis.NewClient(&redis.Options{
			Addr: "r-bp1e9zpgny1jle5itfpd.redis.rds.aliyuncs.com:6379",
			Password: "yebao@2021",
			DB: 9,
		})
	}
	return redisClient
}

// 计算布隆过滤器位图大小
// elemNum 元素个数
// errorRate 误判率
func (b *bloomFilter) getBloomSize() {
	var bloomBitsSize = float64(b.elemNum)*math.Log(b.errRate)/(math.Log(2)*math.Log(2))*(-1)
	b.bitLen = uint64(math.Ceil(bloomBitsSize))
}

// 计算需要的哈希函数数量
// elemNum 元素个数
// bloomSize 布隆过滤器位图大小，单位bit
func (b *bloomFilter) getHashFuncNum() {
	var hNum = math.Log(2)*float64(b.bitLen)/float64(b.elemNum)
	b.hashFunNum = uint64(math.Ceil(hNum))
}

//计算布隆过滤器误判率
// elemNum 元素个数
// bloomSize 布隆过滤器位图大小，单位bit
// hashFuncNum 哈希函数个数
func (b *bloomFilter) getErrRate(elemNum, bloomSize, hashFuncNum uint64) float64 {
	var y = float64(elemNum)*float64(hashFuncNum)/float64(bloomSize)
	return math.Pow(float64(1)-math.Pow(math.E, y*float64(-1)), float64(hashFuncNum))
}


//使用hmac带秘钥的形式，生成随机多个哈希函数
//根据不同的秘钥产生不同的哈希值
func (b *bloomFilter) getHashValue(key, data []byte) int {
	hash:= hmac.New(md5.New, key) // 创建对应的md5哈希加密算法
	hash.Write(data)
	bytesBuffer := bytes.NewBuffer(hash.Sum([]byte("")))
	var tmp uint32
	_ = binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return int(tmp)
}

type bloomFilter struct {
	bitLen uint64 //位长度
	hashFunNum uint64 //哈希函数个数
	elemNum uint64 //预计插入的节点数量
	errRate float64 //误判率
	addr string //redis
	bitRedisKey string //保存在redis里面的位数组的key
	db uint8
}

func NewBloomFilter(errRate float64, redisAddr, bitRedisKey string, elemNum uint64, db uint8) *bloomFilter {
	filter := &bloomFilter{
		elemNum: elemNum,
		errRate: errRate,
		addr: redisAddr,
		bitRedisKey: bitRedisKey,
		db: db,
	}
	//计算位长度
	filter.getBloomSize()
	filter.getHashFuncNum()
	//初始化redis
	filter.connectionRedis()
	//创建位数组到redis里面
	r := redisClient.SetBit(filter.bitRedisKey, int64(filter.bitLen), 0)
	if r.Err() != nil {
		panic("redis init bit arr is fail")
	}
	return filter
}


//增加元素
func (b *bloomFilter) AddElem(elem []byte) error {
	for i := uint64(0); i < b.hashFunNum; i ++ {
		hashVal := b.getHashValue([]byte(strconv.Itoa(int(i))), elem)
		//产生的hash值需要对位数组长度取余
		fmt.Println(int64(hashVal)/int64(b.bitLen))
		//操作redis，修改位值为1
		r := redisClient.SetBit(b.bitRedisKey, int64(hashVal)/int64(b.bitLen), 1)
		if r.Err() != nil {
			return errors.New("redis init bit arr is fail")
		}
	}
	return nil
}


//boolElem 判断元素是否在位数组中
func (b *bloomFilter) BoolElem(elem []byte) bool {
	for i := uint64(0); i < b.hashFunNum; i ++ {
		hashVal := b.getHashValue([]byte(strconv.Itoa(int(i))), elem)
		//产生的hash值需要对位数组长度取余
		//fmt.Println(int64(hashVal)/int64(b.bitLen))
		//操作redis，修改位值为1
		r := redisClient.GetBit(b.bitRedisKey, int64(hashVal)/int64(b.bitLen))
		if r.Err() != nil {
			panic("redis init bit arr is fail")
		}
		b := r.Val()
		fmt.Println(b)
		if r.Val() != 1 {
			return false
		}
	}
	return true
}

//func main() {
//	fmt.Println(Hmac("123", "nihao"))
//	redisClient = connectionRedis()
//	fmt.Println(stringToBin("abc"))
//	//redisClient.SetBit("bolongBit", 6 ,1)
//	//redisClient.SetBit("bolongBit", 7 ,1)
//	redisClient.Set("bolongBit","abc", 0)
//
//	redisClient.SetBit("bolongBit", 6 ,1)
//	redisClient.SetBit("bolongBit", 7 ,0)
//	a := redisClient.Get("bolongBit")
//	b := redisClient.GetBit("bolongBit", 1)
//	fmt.Println(a.Val(), b.Val())
//	//redisClient.Set("bolongBit","", 0)
//}

