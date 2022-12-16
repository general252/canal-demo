package pkg

import (
	"bytes"
	"encoding/json"
	"log"
	"xorm.io/builder"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

var _ canal.EventHandler = &MySQLEventHandler{}

type MySQLEventHandler struct {
	// canal.DummyEventHandler
}

func (h *MySQLEventHandler) actionToSQL(e *canal.RowsEvent) {
	const dialect = builder.SQLITE
	var builds []*builder.Builder

	switch e.Action {
	case canal.InsertAction:
		for i := 0; i < len(e.Rows); i++ {
			object := h.getRowMap(e.Table, e.Rows[i])
			builds = append(builds, builder.Dialect(dialect).Insert(builder.Eq(object)).Into(e.Table.Name))
		}
	case canal.UpdateAction:
		for i := 0; i < len(e.Rows)/2; i++ {
			// _ = h.getRowMap(e.Table, e.Rows[i*2])         // 修改前
			object := h.getRowMap(e.Table, e.Rows[i*2+1]) // 修改后
			builds = append(builds, builder.Dialect(dialect).Update(builder.Eq(object)).From(e.Table.Name).Where(builder.Eq{"id": object["id"]}))
		}

	case canal.DeleteAction:
		for i := 0; i < len(e.Rows); i++ {
			object := h.getRowMap(e.Table, e.Rows[i])
			builds = append(builds, builder.Dialect(dialect).Delete(builder.Eq{"id": object["id"]}).From(e.Table.Name))
		}
	}

	if len(builds) == 0 {
		return
	}

	for _, build := range builds {
		sql, args, err := build.ToSQL()
		if err != nil {
			log.Println(err)
			continue
		}

		data, _ := json.Marshal(args)
		log.Printf("==执行的SQL== sql: [%v] args: [%v]", sql, string(data))
	}
}

func (h *MySQLEventHandler) getRowMap(tab *schema.Table, row []any) map[string]interface{} {
	if len(tab.Columns) != len(row) {
		return nil
	}

	var obj = make(map[string]any)
	for i := 0; i < len(row); i++ {
		obj[tab.Columns[i].Name] = row[i]
	}

	return obj
}

func (h *MySQLEventHandler) OnRow(e *canal.RowsEvent) error {
	log.Printf("OnRow(%v.%v) %v", e.Table.Schema, e.Table.Name, e)
	h.actionToSQL(e)

	switch e.Action {
	case canal.InsertAction:
		// 插入语句
		for i := 0; i < len(e.Rows); i++ {
			data, _ := json.MarshalIndent(h.getRowMap(e.Table, e.Rows[i]), "", "  ")
			log.Println("添加", e.Table.String(), string(data))
		}
	case canal.DeleteAction:
		// 删除语句
		for i := 0; i < len(e.Rows); i++ {
			data, _ := json.MarshalIndent(h.getRowMap(e.Table, e.Rows[i]), "", "  ")
			log.Println("删除", e.Table.String(), string(data))
		}
	case canal.UpdateAction:
		// 修改语句
		for i := 0; i < len(e.Rows)/2; i++ {
			data1, _ := json.MarshalIndent(h.getRowMap(e.Table, e.Rows[i*2]), "", "  ")
			data2, _ := json.MarshalIndent(h.getRowMap(e.Table, e.Rows[i*2+1]), "", "  ")
			log.Printf("[%v] 修改前: %v\n修改后: %v\n", e.Table.String(), string(data1), string(data2))
		}
	default:

	}

	return nil
}

func (h *MySQLEventHandler) OnRotate(e *replication.RotateEvent) error {
	log.Println("Rotate: ", e) // bin-log 滚动 ???
	return nil
}

func (h *MySQLEventHandler) OnTableChanged(schema string, table string) error {
	log.Println("TableChanged: ", schema, table)
	return nil
}

func (h *MySQLEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	// DDL语句
	buf := bytes.Buffer{}
	queryEvent.Dump(&buf)

	log.Println("DDL: ", buf.String())
	return nil
}

func (h *MySQLEventHandler) OnXID(nextPos mysql.Position) error {
	log.Printf("OnXID, pos: %v", nextPos)
	return nil
}

func (h *MySQLEventHandler) OnGTID(gtid mysql.GTIDSet) error {
	log.Printf("OnGTID, set: %v", gtid)
	return nil
}

func (h *MySQLEventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// 同步完成后, 更新本地记录的位置, set是+1, pos增加>=1
	log.Printf("OnPosSynced, pos: %v, set: %v, force: %v", pos, set, force)
	return nil
}

func (h *MySQLEventHandler) String() string {
	return "MySQLEventHandler"
}
