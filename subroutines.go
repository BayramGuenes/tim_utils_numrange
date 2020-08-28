package tim_utils_numrange

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func checkDatabaseAvailable(iConnection string, iSchema string) (eAvailable bool, eException ExceptionStruct) {
	eAvailable = false
	eException = ExceptionStruct{}
	db, err := sql.Open("mysql", iConnection)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	_, err = db.Exec("use " + iSchema)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error() //"no database (Schema=" + iSchema + ")"
		return

	}
	eAvailable = true
	db.Close()
	return
}
func create_table(iConnection string, iSchema string, iTable string, iFields string) (eException ExceptionStruct) {
	eException = ExceptionStruct{}

	db, err := sql.Open("mysql", iConnection+iSchema)
	// fmt.Println(connection+database)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return

	}

	var statement string = "select 1 from " + iTable + " limit 1"
	_, err = db.Exec(statement)
	if err != nil {
		//fmt.Println("*NEWTAB")
		//fmt.Println("Tabelle " + table + " existiert nicht und wird neu angelegt")
		var statement string = "create table " + iTable
		if len(iFields) > 0 {
			statement = statement + " " + iFields + " character set 'utf8'"
		}
		_, err = db.Exec(statement)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(statement)
	}
	db.Close()
	return
}

func insertEntryRangeStartId(iConnection string, iSchema string, iTable string, iStartID int64) (eException ExceptionStruct) {
	eException = ExceptionStruct{}
	db, err := sql.Open("mysql", iConnection+iSchema)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	defer db.Close()
	lvTable := iTable + coTabsuffixRANGESTRTID

	insstmt := "INSERT " + lvTable + " SET startid=?"
	stmt, err := db.Prepare(insstmt)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}

	_, err = stmt.Exec(
		iStartID,
	)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}

	return
}

func getLastIDRangeOffsID(iConnection, iSchema, iTabname string) (eLastId int64, eException ExceptionStruct) {
	eException = ExceptionStruct{}
	eLastId = 0
	db, err := sql.Open("mysql", iConnection+iSchema)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	defer db.Close()

	lTabname := iTabname + coTabsuffixRANGEOFFSID

	headerQuery := "SELECT max(id) as maxid FROM " + lTabname
	rows, err := db.Query(headerQuery)
	defer rows.Close()

	var maxid int64

	for rows.Next() {

		err := rows.Scan(&maxid)
		if err != nil {
			eException.Occured = true
			eException.ErrTxt = err.Error()
			return

		}
		eLastId = maxid

	}

	return
}

func getStartIDRangeStartID(iConnection, iSchema, iTabname string) (eStartId int64, eException ExceptionStruct) {
	eException = ExceptionStruct{}
	eStartId = 0
	db, err := sql.Open("mysql", iConnection+iSchema)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	defer db.Close()

	lTabname := iTabname + coTabsuffixRANGESTRTID
	headerQuery := "SELECT startid FROM " + lTabname
	rows, err := db.Query(headerQuery)
	defer rows.Close()

	var startid int64

	for rows.Next() {

		err := rows.Scan(&startid)
		if err != nil {
			eException.Occured = true
			eException.ErrTxt = err.Error()
			return
		}
		eStartId = startid

	}

	return
}

func insertEntryIntoRangeOffsID(iConnection, iSchema, iTabname string) (eLastId int64, eException ExceptionStruct) {
	eException = ExceptionStruct{}
	eLastId = 0

	db, err := sql.Open("mysql", iConnection+iSchema)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	defer db.Close()

	lTabname := iTabname + coTabsuffixRANGEOFFSID

	insstmt := "INSERT " + lTabname + " SET lastupdate=?"

	currentTime := time.Now()
	timePostfix := currentTime.Format("20060102150405")

	stmt, err := db.Prepare(insstmt)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}

	//stmt, err := db.Prepare("INSERT table SET unique_id=? ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)")

	res, err := stmt.Exec(timePostfix)
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	lid, err := res.LastInsertId()
	if err != nil {
		eException.Occured = true
		eException.ErrTxt = err.Error()
		return
	}
	eLastId = lid
	return
}
