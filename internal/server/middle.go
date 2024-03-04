package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/urfave/negroni"
)

type JsonFormatter struct{}

func (t *JsonFormatter) FormatPanicError(rw http.ResponseWriter, r *http.Request, infos *negroni.PanicInformation) {
	if rw.Header().Get("Content-Type") != "application/json" {
		rw.Header().Set("Content-Type", "application/json")
	}
	resp, _ := json.Marshal(httpResponse{
		Code:    -1,
		Message: fmt.Sprintf("%v\n", infos.RecoveredPanic),
	})
	_, _ = rw.Write(resp)
}