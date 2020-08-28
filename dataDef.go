package tim_utils_numrange

const (
	coTabsuffixRANGEOFFSID = "__NMRANGEOFFSID"
	coTabsuffixRANGESTRTID = "__NMRANGESTRTID"
)

type NumRange struct {
	DBServerAdress string
	DBServerPort   string
	DBUsrPwd       string
	DBSchemaName   string
	DBConnection   string
}

type ExceptionStruct struct {
	Occured bool
	ErrTxt  string
}

type OutParamCreateNumRange struct {
	TabnameNumRange string
	Exception       ExceptionStruct
}

type OutParamDisplayNumRange struct {
	TabOwner              string
	TabnameNumRange       string
	NumRangeStartInt      int64
	NumRangeCurrentOffset int64
	LastIDTabOwner        int64
	Exception             ExceptionStruct
}
type OutParamGetNextNumber struct {
	Number    int64
	Exception ExceptionStruct
}
