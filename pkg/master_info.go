package pkg

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type DBParam struct {
	Host     string
	Port     int
	User     string
	Password string
}

type MasterDBServerInfo struct {
	BinlogFileName string
	BinlogPosition int64

	Variables map[string]string
	Status    map[string]string

	param DBParam
}

func NewMasterDBServerInfo(param DBParam) *MasterDBServerInfo {
	return &MasterDBServerInfo{
		param:     param,
		Variables: map[string]string{},
		Status:    map[string]string{},
	}
}

func (tis *MasterDBServerInfo) String() string {
	masterInfo := tis
	data, _ := json.MarshalIndent(map[string]any{
		"binlogFile":               masterInfo.BinlogFileName,
		"binlogPosition":           masterInfo.BinlogPosition,
		"log_bin":                  masterInfo.GetStringMust("log_bin"),
		"gtid_mode":                masterInfo.GetStringMust("gtid_mode"),
		"server_uuid":              masterInfo.GetStringMust("server_uuid"),
		"enforce_gtid_consistency": masterInfo.GetStringMust("enforce_gtid_consistency"),
		"gtid_executed":            masterInfo.GetStringMust("gtid_executed"),
		"gtid_current_pos":         masterInfo.GetStringMust("gtid_current_pos"),
		"gtid_purged":              masterInfo.GetStringMust("gtid_purged"),
	}, "", "  ")

	return string(data)
}

func (tis *MasterDBServerInfo) UpdateData() error {
	cfg := tis.param

	// SHOW GLOBAL VARIABLES LIKE '%gtid%';

	var queryGlobalValue = func(conn *client.Conn, sql string, on func(r *mysql.Result) error) error {
		if r, err := conn.Execute(sql); err != nil {
			return err
		} else {
			err = on(r)
			r.Close()
			return err
		}
	}

	var stringCopy = func(value string) string {
		var sb strings.Builder
		sb.WriteString(value)

		return sb.String()
	}

	var (
		err  error
		conn *client.Conn
	)

	// 连接数据库
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	if conn, err = client.Connect(addr, cfg.User, cfg.Password, "information_schema"); err != nil {
		return err
	} else {
		defer func() {
			_ = conn.Close()
		}()
	}

	for i := 0; i < 1; i++ {
		_ = queryGlobalValue(conn, "SHOW GLOBAL VARIABLES", func(r *mysql.Result) error {

			for row := 0; row < r.RowNumber(); row++ {
				key, err1 := r.GetString(row, 0)
				value, err2 := r.GetString(row, 1)
				if err1 != nil || err2 != nil {
					log.Printf("[%v] err1: %v, err2: %v", row, err1, err2)
					continue
				}

				tis.Variables[stringCopy(key)] = stringCopy(value)
			}

			return nil
		})

		_ = queryGlobalValue(conn, "SHOW GLOBAL STATUS", func(r *mysql.Result) error {

			for row := 0; row < r.RowNumber(); row++ {
				key, err1 := r.GetString(row, 0)
				value, err2 := r.GetString(row, 1)
				if err1 != nil || err2 != nil {
					log.Printf("[%v] err1: %v, err2: %v", row, err1, err2)
					continue
				}

				tis.Status[stringCopy(key)] = stringCopy(value)
			}

			return nil
		})

		if err = queryGlobalValue(conn, "SHOW MASTER STATUS", func(r *mysql.Result) error {
			if value, err := r.GetStringByName(0, "File"); err != nil {
				return err
			} else if value == "NONE" {
				return fmt.Errorf("no value")
			} else {
				// 深度复制
				var sb strings.Builder
				sb.WriteString(value)

				tis.BinlogFileName = sb.String()
			}

			if value, err := r.GetIntByName(0, "Position"); err != nil {
				return err
			} else if value == 0 {
				return fmt.Errorf("no value")
			} else {
				tis.BinlogPosition = value
			}

			return nil
		}); err != nil {
			break
		}

		if true {
			break
		}
	}

	return nil
}

func (tis *MasterDBServerInfo) GetString(name string) (string, bool) {
	v, ok := tis.Variables[name]
	return v, ok
}

func (tis *MasterDBServerInfo) GetStringMust(name string) string {
	v, _ := tis.GetString(name)
	return v
}

func (tis *MasterDBServerInfo) GetInt(name string) (int, bool) {
	v, ok := tis.Variables[name]
	if !ok {
		return 0, false
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}

	return i, true
}

func (tis *MasterDBServerInfo) GetIntMust(name string) int {
	v, _ := tis.GetInt(name)
	return v
}

// GetGTIDSet GTID Set
func (tis *MasterDBServerInfo) GetGTIDSet() (string, bool) {
	if v, ok := tis.GetString("gtid_executed"); ok {
		return v, true
	}
	if v, ok := tis.GetString("gtid_current_pos"); ok {
		return v, true
	}

	return "", false
}
