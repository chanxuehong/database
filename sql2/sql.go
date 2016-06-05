package sql2

import (
	"database/sql"
	"sync"

	"github.com/chanxuehong/database/sql2/lru"
)

const DefaultCacheSize = 1024

type DB struct {
	*sql.DB

	stmtCacheMutex sync.Mutex // used only by writers
	stmtCache      *lru.Cache
}

func NewDB(db *sql.DB) *DB {
	return &DB{
		DB:        db,
		stmtCache: lru.New(DefaultCacheSize),
	}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

func (db *DB) SetCacheSize(size int) {
	if size <= 0 {
		size = DefaultCacheSize
	}
	db.stmtCache.SetSize(size)
}

func (db *DB) Prepare(query string) (stmt Stmt, err error) {
	value, err := db.stmtCache.Get(lru.Key(query))
	if err != nil {
		if err != lru.ErrNotFound {
			return
		}
	} else {
		stmt = Stmt(value)
		return
	}

	// err == lru.ErrNotFound

	db.stmtCacheMutex.Lock()
	defer db.stmtCacheMutex.Unlock()

	value, err = db.stmtCache.Get(lru.Key(query))
	if err != nil {
		if err != lru.ErrNotFound {
			return
		}
	} else {
		stmt = Stmt(value)
		return
	}

	sqlStmt, err := db.DB.Prepare(query)
	if err != nil {
		return
	}
	stmt = Stmt{Stmt: sqlStmt}

	db.stmtCache.Add(lru.Key(query), lru.Value(stmt))
	return
}

// ================================================================================================================

type Stmt struct {
	*sql.Stmt
}

func (Stmt) Close() error {
	return nil
}
