package tim_utils_numrange

import _ "github.com/go-sql-driver/mysql"

func NewManager(iDBServerAdress, iDBServerPort, iDBUsrPwd, iDBSchema string) (eNr NumRange) {
	lONr := NumRange{}
	lONr.DBServerAdress = iDBServerAdress
	lONr.DBServerPort = iDBServerPort
	lONr.DBUsrPwd = iDBUsrPwd
	lONr.DBSchemaName = iDBSchema
	lONr.DBConnection = lONr.DBUsrPwd + "@tcp(" + lONr.DBServerAdress + ":" +
		lONr.DBServerPort + ")/"
	return lONr

}

func (nr NumRange) CreateNumRange(iTabname string, iNumRangeStartInt int64) (eOutput OutParamCreateNumRange) {
	eOutput = OutParamCreateNumRange{}

	// =======================checkDatabaseAvailable ==================== //

	available, lException := checkDatabaseAvailable(nr.DBConnection, nr.DBSchemaName)
	eOutput.Exception = lException
	if !lException.Occured && !available {
		eOutput.Exception.Occured = true
		eOutput.Exception.ErrTxt = "Database(Schema) " + nr.DBSchemaName + " doesnot exist."
		return
	}

	// =======================create_table Tabname+coTabsuffixRANGEOFFSID ============ //

	lvTable := iTabname + coTabsuffixRANGEOFFSID
	lvFields := "(" +
		"id bigint not null auto_increment," +
		"lastupdate varchar(20) not null, " +
		"primary key (id))"
	lException = create_table(nr.DBConnection, nr.DBSchemaName, lvTable, lvFields)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	// =======================insert entry into Tabname+coTabsuffixRANGEOFFSID ============ //

	_, lException = insertEntryIntoRangeOffsID(nr.DBConnection, nr.DBSchemaName, iTabname)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	// =======================create_tableTabname + coTabsuffixRANGESTRTID ============ //

	lvTable = iTabname + coTabsuffixRANGESTRTID
	lvFields = "(" +
		"startid bigint not null" +
		")"
	lException = create_table(nr.DBConnection, nr.DBSchemaName, lvTable, lvFields)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}

	// =======================insert entry into tableTabname + coTabsuffixRANGESTRTID ============ //

	lException = insertEntryRangeStartId(nr.DBConnection, nr.DBSchemaName, iTabname, iNumRangeStartInt)
	eOutput.Exception = lException

	return eOutput
}

func (nr NumRange) DisplayNumRange(iTabname string) (eOutput OutParamDisplayNumRange) {
	eOutput = OutParamDisplayNumRange{}

	// =======================checkDatabaseAvailable ==================== //

	available, lException := checkDatabaseAvailable(nr.DBConnection, nr.DBSchemaName)
	eOutput.Exception = lException
	if !lException.Occured && !available {
		eOutput.Exception.Occured = true
		eOutput.Exception.ErrTxt = "Database(Schema) " + nr.DBSchemaName + " doesnot exist."
		return
	}

	lfExistNR, lException := nr.ExistsNumRange(nr.DBSchemaName, iTabname)
	if !lfExistNR {
		eOutput.Exception.Occured = true
		eOutput.Exception.ErrTxt = "Nurrmernkreis existiert nicht!"
		return
	}
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}

	lLastId, lException := getLastIDRangeOffsID(nr.DBConnection, nr.DBSchemaName, iTabname)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	eOutput.NumRangeCurrentOffset = lLastId

	lStartId, lException := getStartIDRangeStartID(nr.DBConnection, nr.DBSchemaName, iTabname)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	eOutput.NumRangeStartInt = lStartId
	eOutput.LastIDTabOwner = eOutput.NumRangeCurrentOffset + eOutput.NumRangeStartInt

	return eOutput
}

func (nr NumRange) ExistsNumRange(iSchema, iTabname string) (eExists bool, eException ExceptionStruct) {
	eExists = false
	eException = ExceptionStruct{}

	available, lException := checkDatabaseAvailable(nr.DBConnection, nr.DBSchemaName)
	eException = lException
	if !lException.Occured && !available {
		eException.Occured = true
		eException.ErrTxt = "Database(Schema) " + nr.DBSchemaName + " doesnot exist."
		return
	}

	lTabname := iTabname + coTabsuffixRANGEOFFSID
	lExists, lException := checkDBTableAvailable(nr.DBConnection, nr.DBSchemaName, lTabname)
	eException = lException
	if !lException.Occured && !lExists {
		eException.Occured = true
		eException.ErrTxt = "Table " + iTabname + " in Database(Schema) " + nr.DBSchemaName + " doesnot exist."
		return
	}

	if !lExists {
		return
	}

	lTabname = iTabname + coTabsuffixRANGESTRTID

	eExists, lException = checkDBTableAvailable(nr.DBConnection, nr.DBSchemaName, lTabname)
	eException = lException
	if !lException.Occured && !eExists {
		eException.Occured = true
		eException.ErrTxt = "Table " + iTabname + " in Database(Schema) " + nr.DBSchemaName + " doesnot exist."
		return
	}
	return
}

func (nr NumRange) GetNextNumber(iTabname string) (eOutput OutParamGetNextNumber) {
	eOutput = OutParamGetNextNumber{}

	lfExistNR, lException := nr.ExistsNumRange(nr.DBSchemaName, iTabname)
	if !lfExistNR {
		eOutput.Exception.Occured = true
		eOutput.Exception.ErrTxt = "Nurrmernkreis existiert nicht!"
		return
	}
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	lStartId, lException := getStartIDRangeStartID(nr.DBConnection, nr.DBSchemaName, iTabname)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	lLastId, lException := insertEntryIntoRangeOffsID(nr.DBConnection, nr.DBSchemaName, iTabname)
	eOutput.Exception = lException
	if eOutput.Exception.Occured {
		return
	}
	eOutput.Number = lLastId + lStartId
	return eOutput
}
