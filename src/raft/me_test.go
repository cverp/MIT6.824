package raft

import (
	"testing"
	"encoding/json"
	"fmt"
)

type header struct {
	encryption  string
	timestamp   int64
	key         string
	Partnercode int
 }

func TestMe(t *testing.T) {
   //转换成JSON字符串
	headerTest := header{
		encryption:  "sha",
		timestamp:   1482463793,
		key:         "2342874840784a81d4d9e335aaf76260",
		Partnercode: 10025,
	}
	jsons, errs := json.Marshal(headerTest) //转换成JSON返回的是byte[]
	if errs != nil {
   		fmt.Println(errs.Error())
	}
	fmt.Println(string(jsons)) //byte[]转换成string 输出
}
