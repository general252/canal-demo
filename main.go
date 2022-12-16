package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/general252/canal-demo/pkg"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	var (
		err   error
		wg    sync.WaitGroup
		param = pkg.DBParam{
			Host:     "127.0.0.1",
			Port:     9706,
			User:     "root",
			Password: "123456",
		}
		masterInfo = pkg.NewMasterDBServerInfo(param)
	)

	// 获取信息
	if err = masterInfo.UpdateData(); err != nil {
		log.Println(err)
		return
	} else {
		log.Println(masterInfo.String())
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", param.Host, param.Port)
	cfg.User = param.User
	cfg.Password = param.Password
	cfg.Flavor = mysql.MySQLFlavor
	cfg.UseDecimal = true
	cfg.Charset = "utf8mb4"
	cfg.SemiSyncEnabled = false // 默认异步复制, 安装插件可支持半同步复制(主库提交操作会被阻止直到至少有一个半同步的复制slave确认依据接收到本次更新操作并写到relay log中，主库的提交操作才会继续)
	cfg.Dump.ExecutionPath = ""

	// 这个判断是本地判断
	cfg.IncludeTableRegex = []string{
		"students\\..*",
		"test_log\\..*",
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create canal err %v", err)
		return
	}

	c.SetEventHandler(&pkg.MySQLEventHandler{})

	wg.Add(1)
	go func() {
		defer wg.Done()

		if false {
			err = c.RunFrom(mysql.Position{
				Name: masterInfo.BinlogFileName,
				Pos:  uint32(masterInfo.BinlogPosition),
			})
			log.Println(err)
		} else {
			// s = "3f468344-675e-11eb-b210-80ce62f266bc:1-318197", 表示此程序已经同步到哪里了
			s, _ := masterInfo.GetGTIDSet()

			set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, s)
			if err != nil {
				log.Println(err)
				return
			}

			err = c.StartFromGTID(set)
			log.Println(err)
		}
	}()

	sc := make(chan os.Signal, 2)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGHUP,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc

	c.Close()
	wg.Wait()
}
