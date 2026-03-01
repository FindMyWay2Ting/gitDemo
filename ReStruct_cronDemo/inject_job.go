package main

import (
	"context"
	"encoding/json"
	"fmt"
	"my-cron/common"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	// 连接 Etcd
	client, _ := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})

	// 构造一个“一定会失败”的任务，测试重试机制
	job := common.Job{
		Name:          "test_retry_job",
		CronExpr:      "10 * * * * *", // 每5秒一次
		Type:          "http",
		HttpUrl:       "http://127.0.0.1:8888/test", // 连刚才的 debug_server
		HttpMethod:    "GET",
		Timeout:       2000,
		RetryCount:    3,    // 🔥 重点：要求重试3次
		RetryInterval: 1000, // 每次间隔1秒
	}

	// 序列化
	bytes, _ := json.Marshal(job)

	// 写入 Etcd
	kv := clientv3.NewKV(client)
	kv.Put(context.TODO(), "/cron/jobs/test_retry_job", string(bytes))

	fmt.Println("任务已写入 Etcd，请观察 Worker 控制台...")
}
