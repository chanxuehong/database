package sql

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"unsafe"
)

type DB struct {
	*sql.DB

	stmtMapPtrMutex sync.Mutex     // used only by writers
	stmtMapPtr      unsafe.Pointer // *stmtMap
}

type stmtMap map[string]Stmt // map[query]Stmt

func NewDB(db *sql.DB) *DB {
	return &DB{
		DB: db,
	}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

func (db *DB) Prepare(query string) (stmt Stmt, err error) {
	var m stmtMap
	if p := (*stmtMap)(atomic.LoadPointer(&db.stmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	db.stmtMapPtrMutex.Lock()
	defer db.stmtMapPtrMutex.Unlock()

	if p := (*stmtMap)(atomic.LoadPointer(&db.stmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	stmtx, err := db.DB.Prepare(query)
	if err != nil {
		return
	}
	stmt = Stmt{Stmt: stmtx}

	m2 := make(stmtMap, len(m)+1)
	for k, v := range m {
		m2[k] = v
	}
	m2[query] = stmt

	atomic.StorePointer(&db.stmtMapPtr, unsafe.Pointer(&m2))
	return
}

// =====================================================================================================================

type Stmt struct {
	*sql.Stmt
}

func (s Stmt) Close() error {
	return nil
}
