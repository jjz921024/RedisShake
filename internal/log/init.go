package log

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func Init(level string, file string, dir string) {
	// log level
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	default:
		panic(fmt.Sprintf("unknown log level: %s", level))
	}

	// dir
	dir, err := filepath.Abs(dir)
	if err != nil {
		panic(fmt.Sprintf("failed to determine current directory: %v", err))
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0777)
		if err != nil {
			panic(fmt.Sprintf("mkdir failed. dir=[%s], error=[%v]", dir, err))
		}
	}
	path := filepath.Join(dir, file)

	// log file
	/* consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	fileWriter, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("open log file failed. file=[%s], err=[%s]", path, err))
	}
	multi := zerolog.MultiLevelWriter(consoleWriter, fileWriter) */

	// 日志切割
	fileRotate := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    200,    // M为单位，达到这个设置数后就进行日志切割
		MaxBackups: 10,     // 保留旧文件最大份数
		MaxAge:     7,      // 旧文件最大保存天数
		Compress:   true,   // disabled by default，是否压缩日志归档，默认不压缩
	}

	logger = zerolog.New(fileRotate).With().Timestamp().Logger()
	Infof("log_level: [%v], log_file: [%v]", level, path)
}
