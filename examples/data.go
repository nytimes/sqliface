package models

import (
	"database/sql"

	"github.com/nytimes/gizmo/config/mysql"

	"github.com/nytimes/sqliface"
)

type (
	DataRepo interface {
		Save(Data) error
		Get(uint64) (Data, error)
		List(uint64) ([]Data, error)
	}

	Data struct {
		ID   uint64
		Name string
	}

	MySQLDataRepo struct {
		db sqliface.ExecCloser
	}
)

func NewMySQLDataRepo(cfg *mysql.Config) (DataRepo, error) {
	db, err := cfg.DB()
	return &MySQLDataRepo{db}, err
}

func (s *MySQLDataRepo) Save(data Data) error {
	_, err := s.db.Exec(`INSERT INTO data (id,name) VALUES (?, ?)
							ON DUPLICATE KEY UPDATE name = ?`,
		data.ID,
		data.Name,
		data.Name)
	return err
}

func (s *MySQLDataRepo) Get(id uint64) (Data, error) {
	row := s.db.QueryRow(`
			SELECT id, name
			FROM data
			WHERE id = ?`, id)

	var data Data
	err := scanData(row, &data)
	return data, err
}

func (s *MySQLDataRepo) List(limit uint64) ([]Data, error) {
	rows, err := s.db.Query(`
			SELECT id, name
			FROM data
			WHERE status = 1
			LIMIT ?`, limit)
	if err != nil {
		return []Data{}, err
	}
	return scanDatas(rows)
}

func (s *MySQLDataRepo) Close() error {
	return s.db.Close()
}

func scanDatas(rows sqliface.Rows) (datas []Data, err error) {
	var data Data
	for rows.Next() {
		err = scanData(rows, &data)
		if err != nil {
			rows.Close()
			return datas, err
		}

		datas = append(datas, data)
	}
	return datas, rows.Err()
}

func scanData(row sqliface.Row, data *Data) error {
	// assuming 'name' is nullable, lets use sql.NullString
	var name sql.NullString
	err := row.Scan(
		&data.ID,
		&name,
	)
	data.Name = name.String
	return err
}
